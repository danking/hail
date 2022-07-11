//! This is an example of the [Kaleidoscope tutorial](https://llvm.org/docs/tutorial/)
//! made in Rust, using Inkwell.
//! Currently, all features up to the [7th chapter](https://llvm.org/docs/tutorial/LangImpl07.html)
//! are available.
//! This example is supposed to be ran as a executable, which launches a REPL.
//! The source code is in the following order:
//! - Lexer,
//! - Parser,
//! - Compiler,
//! - Program.
//!
//! Both the `Parser` and the `Compiler` may fail, in which case they would return
//! an error represented by `Result<T, &'static str>`, for easier error reporting.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::{self, Write};
use std::iter::Peekable;
use std::ops::DerefMut;
use std::str::Chars;

use inkwell::builder::Builder;
use inkwell::context::Context;
use inkwell::module::Module;
use inkwell::passes::PassManager;
use inkwell::types::BasicMetadataTypeEnum;
use inkwell::values::{BasicMetadataValueEnum, BasicValue, FloatValue, FunctionValue, PointerValue};
use inkwell::{FloatPredicate, OptimizationLevel};


pub enum BinaryOp {
    Add(),
}

#[derive(Debug)]
pub enum IR {
    I32(i32),
    ApplyBinaryPrimOp {
        op: BinaryOp,
        left: Box<IR>,
        right: Box<IR>,
    },
}

pub fn parse(input: &str) -> Result<IR> {
    let chars = Box::new(input.chars().peekable());
    let pos = 0
    loop {
        loop {
            let ch = chars.peek() {
                if ch.is_none(){
                    return Err("unexpected end of stream");
                }
                if !ch.unwrap().is_whitespace() {
                    break;
                }
            }
        }
    }
}
