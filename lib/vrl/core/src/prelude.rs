// commonly used modules
pub use compiler::{expression, state, value::kind};
// pub use crate::{expression, function, state, value};

// commonly used top-level crate types
pub use compiler::{
    value::{Collection, Field, Index, Kind},
    Context, Expression, ExpressionError, Function, Resolved, Target, TypeDef, Value,
};

pub type Result<T> = std::result::Result<T, ExpressionError>;

pub use std::collections::BTreeMap;
pub use std::fmt;

pub use bytes::Bytes;
// pub use crate::{Error, Expr, Expression, Function, Object, Result, TypeDef, Value};

// commonly used expressions

// pub use compiler::expression::Resolved;

// commonly used function types
pub use compiler::function::{
    ArgumentList, Compiled, CompiledArgument, Example, FunctionCompileContext, Parameter,
};
// commonly used macros
pub use compiler::{
    bench_function, expr, expression::FunctionArgument, func_args, map, test_function,
    test_type_def, type_def, value, vm::VmArgumentList,
};
pub use diagnostic::DiagnosticError;
pub use indoc::indoc;
pub use ordered_float::NotNan;
// pub use crate::{array, bench_function, func_args, lit, map, test_function, test_type_def};
