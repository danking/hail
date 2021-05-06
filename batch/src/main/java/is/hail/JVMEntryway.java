package is.hail;

import java.util.*;
import java.lang.reflect.*;
import java.net.*;
import java.nio.*;
import java.io.*;
import org.newsclub.net.unix.demo.DemoHelper;
import org.newsclub.net.unix.server.AFUNIXSocketServer;

class JVMEntryway {
  private static final HashMap<String, ClassLoader> classLoaders = new HashMap<>();

  public static void main(String[] args) throws Exception {
    assert(args.length == 1);
    ServerSocket server = AFUnixServerSocket.newInstance();
    server.bindOn(new File(args[0]), true);
    while (true) {
      Socket sock = server.accept();
    }
    socket.setSoTimeout(60000);
    DataInputStream in = new DataInputStream(socket.getInputStream());
    int nArgs = in.readInt();
    String[] realArgs = new String[nArgs];
    for (int i = 0; i < nArgs; ++i) {
      val length = in.readInt();
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
    main.invoke(null, (Object) realArgs);
    Thread.currentThread().setContextClassLoader(oldClassLoader);
  }
}

