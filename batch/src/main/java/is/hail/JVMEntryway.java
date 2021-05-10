package is.hail;

import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.nio.*;
import java.nio.charset.*;
import java.util.*;
import org.newsclub.net.unix.*;

class JVMEntryway {
  private static final HashMap<String, ClassLoader> classLoaders = new HashMap<>();

  public static void main(String[] args) throws Exception {
    assert(args.length == 1);
    AFUNIXServerSocket server = AFUNIXServerSocket.newInstance();
    server.bindOn(new AFUNIXSocketAddress(new File(args[0])));
    while (true) {
      AFUNIXSocket socket = server.accept();
      socket.setSoTimeout(60000);
      DataInputStream in = new DataInputStream(socket.getInputStream());
      DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      int nArgs = in.readInt();
      String[] realArgs = new String[nArgs];
      for (int i = 0; i < nArgs; ++i) {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.read(bytes);
        realArgs[i] = new String(bytes);
      }

      assert(realArgs.length >= 2);
      String classPath = realArgs[0];
      String mainClass = realArgs[1];
      ClassLoader cl = classLoaders.get(classPath);
      if (cl == null) {
        String[] urlStrings = classPath.split(",");
        ArrayList<URL> urls = new ArrayList<>();
        for (int i = 0; i < urlStrings.length; ++i) {
          File file = new File(urlStrings[i]);
          urls.add(file.toURI().toURL());
          if (file.isDirectory()) {
            for (final File f : file.listFiles()) {
              urls.add(f.toURI().toURL());
            }
          }
        }
        cl = new URLClassLoader(urls.toArray(new URL[0]));
        classLoaders.put(classPath, cl);
      }
      Class<?> klass = cl.loadClass(mainClass);
      Method main = klass.getDeclaredMethod("main", String[].class);
      ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(cl);

      Boolean invoked = false;
      try {
        main.invoke(null, (Object) realArgs);
        invoked = true;
        out.writeBoolean(true);
      } catch (Throwable t) {
        if (invoked) {
          throw t;
        }

        out.writeBoolean(false);
        try (StringWriter sw = new java.io.StringWriter();
             PrintWriter pw = new java.io.PrintWriter(sw)) {
          t.printStackTrace(pw);
          String s = sw.toString();
          byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
          out.writeInt(bytes.length);
          out.write(bytes);
        }
      }
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }
  }
}

