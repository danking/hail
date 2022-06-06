For Mac OS X, install llvm:

```
brew install llvm
```

Use cmake to generate a Makefile. You must inform cmake of LLVM's location. You also must specify a
recent C++ compiler. This works for me with LLVM 13:

```
LLVM_DIR=/usr/local/opt/llvm/lib/cmake/ cmake . -D CMAKE_CXX_COMPILER=/usr/local/opt/llvm/bin/clang++ -D CMAKE_C_COMPILER=/usr/local/opt/llvm/bin/clang
```

Now you can actually make the project:

```
make
```
