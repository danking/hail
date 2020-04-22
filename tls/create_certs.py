import argparse
import base64
import os
import json
import yaml
import shutil
import subprocess as sp
import tempfile

parser = argparse.ArgumentParser(prog='create_certs.py',
                                 description='create hail certs')
parser.add_argument('namespace', type=str, help='kubernetes namespace')
parser.add_argument('config_file', type=str, help='YAML format config file')
parser.add_argument('root_key_file', type=str, help='the root key file')
parser.add_argument('root_cert_file', type=str, help='the root cert file')

args = parser.parse_args()
namespace = args.namespace
with open(args.config_file) as f:
    arg_config = yaml.safe_load(f)
root_key_file = args.root_key_file
root_cert_file = args.root_cert_file


def create_key_and_cert(p):
    name = p['name']
    domain = p['domain']
    unmanaged = p.get('unmanaged', False)
    if unmanaged and namespace != 'default':
        return
    key_file = f'{name}-key.pem'
    csr_file = f'{name}-csr.csr'
    cert_file = f'{name}-cert.pem'
    names = [
        domain,
        f'{domain}.{namespace}',
        f'{domain}.{namespace}.svc.cluster.local'
    ]
    sp.check_call([
        'openssl', 'genrsa',
        '-out', key_file,
        '-nodes',  # no password, key itself is cleartext
        '4096'
    ])
    sp.check_call([
        'openssl', 'req',
        '-new',
        '-subj', f'/CN={names[0]}',
        '-addext', f'subjectAltName = {",".join("DNS:" + n for n in names)}',
        '-key', key_file,
        '-out', csr_file
    ])
    sp.check_call([
        'openssl', 'x509',
        '-req',
        '-in', csr_file,
        '-CA', root_cert_file,
        '-CAkey', root_key_file,
        '-CAcreateserial',
        '-out', cert_file,
        '-days', 365
    ])
    return {'key': key_file, 'cert': cert_file}


def create_trust(principal, trust_type):  # pylint: disable=unused-argument
    trust_file = f'{principal}-{trust_type}.pem'
    with open(trust_file, 'w') as out:
        # FIXME: mTLS, only trust certain principals
        with open(root_cert_file, 'r') as root_cert:
            shutil.copyfileobj(root_cert, out)
    return trust_file


def create_json_config(principal, incoming_trust, outgoing_trust, key, cert):
    principal_config = {
        'outgoing_trust': f'/ssl-config/{outgoing_trust}',
        'incoming_trust': f'/ssl-config/{incoming_trust}',
        'key': f'/ssl-config/{key}',
        'cert': f'/ssl-config/{cert}'
    }
    config_file = f'ssl-config.json'
    with open(config_file, 'w') as out:
        out.write(json.dumps(principal_config))
        return [config_file]


def create_nginx_config(principal, incoming_trust, outgoing_trust, key, cert):
    http_config_file = f'ssl-config-http.conf'
    proxy_config_file = f'ssl-config-proxy.conf'
    with open(proxy_config_file, 'w') as proxy, open(http_config_file, 'w') as http:
        proxy.write(f'proxy_ssl_certificate         /ssl-config/{cert};\n')
        proxy.write(f'proxy_ssl_certificate_key     /ssl-config/{key};\n')
        proxy.write(f'proxy_ssl_trusted_certificate /ssl-config/{outgoing_trust};\n')
        proxy.write(f'proxy_ssl_verify              on;\n')
        proxy.write(f'proxy_ssl_verify_depth        1;\n')
        proxy.write(f'proxy_ssl_session_reuse       on;\n')

        http.write(f'ssl_certificate /ssl-config/{cert};\n')
        http.write(f'ssl_certificate_key /ssl-config/{key};\n')
        http.write(f'ssl_client_certificate /ssl-config/{incoming_trust};\n')
        http.write(f'ssl_verify_client optional;\n')
        # FIXME: mTLS
        # http.write(f'ssl_verify_client on;\n')
    return [http_config_file, proxy_config_file]


def create_curl_config(principal, incoming_trust, outgoing_trust, key, cert):
    if os.stat(incoming_trust).st_size != 0:
        raise ValueError(f'curl accepts no connections so incoming trust is '
                         f'nonsensical, but was: {incoming_trust}')
    config_file = f'ssl-config.curlrc'
    with open(config_file, 'w') as out:
        out.write(f'key       /ssl-config/{key}\n')
        out.write(f'cert      /ssl-config/{cert}\n')
        out.write(f'cacert    /ssl-config/{outgoing_trust}\n')
    return [config_file]


def create_config(principal, incoming_trust, outgoing_trust, key, cert, kind):
    if kind == 'json':
        return create_json_config(principal, incoming_trust, outgoing_trust, key, cert)
    if kind == 'curl':
        return create_curl_config(principal, incoming_trust, outgoing_trust, key, cert)
    assert kind == 'nginx'
    return create_nginx_config(principal, incoming_trust, outgoing_trust, key, cert)


def create_principal(principal, domain, kind, key, cert, unmanaged):
    if unmanaged and namespace != 'default':
        return
    incoming_trust = create_trust(principal, 'incoming')
    outgoing_trust = create_trust(principal, 'outgoing')
    configs = create_config(principal, incoming_trust, outgoing_trust, key, cert, kind)
    with tempfile.NamedTemporaryFile() as k8s_secret:
        sp.check_call(
            ['kubectl', 'create', 'secret', 'generic', f'ssl-config-{principal}',
             f'--namespace={namespace}',
             f'--from-file={key}',
             f'--from-file={cert}',
             f'--from-file={incoming_trust}',
             f'--from-file={outgoing_trust}',
             *[f'--from-file={c}' for c in configs],
             '--dry-run', '-o', 'yaml'],
            stdout=k8s_secret)
        sp.check_call(['kubectl', 'apply', '-f', k8s_secret.name])


def download_previous_certs():
    for p in arg_config['principals']:
        name = p["name"]
        unmanaged = p.get('unmanaged', False)
        if unmanaged and namespace != 'default':
            config_source_namespace = 'default'
        else:
            config_source_namespace = namespace
        result = sp.run(
            ['kubectl', 'get', 'secret', f'ssl-config-{name}',
             f'--namespace={config_source_namespace}', '-o', 'json'],
            stderr=sp.PIPE,
            stdout=sp.PIPE)
        if result.returncode == 1:
            if f'Error from server (NotFound)' in result.stderr.decode():
                cert = b''
            else:
                raise ValueError(f'something went wrong: {result.stderr.decode()}\n---\n{result.stdout.decode()}')
        else:
            secret = json.loads(result.stdout.decode())
            cert = base64.standard_b64decode(secret['data'][f'{name}-cert.pem'])
        with open(f'previous-{name}-cert.pem', 'wb') as f:
            f.write(cert)


assert 'principals' in arg_config, arg_config

principal_by_name = {
    p['name']: p
    for p in arg_config['principals']
}

principal_by_name = {
    p['name']: {**p,
                **create_key_and_cert(p)}
    for p in arg_config['principals']
}
download_previous_certs()
for name, p in principal_by_name.items():
    create_principal(name,
                     p['domain'],
                     p['kind'],
                     p['key'],
                     p['cert'],
                     p.get('unmanaged', False))
