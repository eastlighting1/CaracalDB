import pyarrow as pa
import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.proc import (
    AssignStep,
    CallStep,
    ForStep,
    IfStep,
    Procedure,
    WhileStep,
)
from caracaldb.udf import PyUdf, UdfRegistry, define_tuft_udf, udf

# ---------------------------------------------------------------------------
# Tuft UDFs
# ---------------------------------------------------------------------------


def test_tuft_udf_substitutes_param_into_expression() -> None:
    fn = define_tuft_udf("eq_chr", ["target"], ("eq", ("col", "chromosome"), ("param", "target")))
    batch = pa.record_batch({"chromosome": pa.array(["17", "12", "17"])})
    out = fn(batch, ("lit", "17"))
    assert out.to_pylist() == [True, False, True]


def test_tuft_udf_rejects_arity_mismatch() -> None:
    fn = define_tuft_udf("noop", ["x"], ("param", "x"))
    with pytest.raises(CaracalError) as exc:
        fn(pa.record_batch({"x": pa.array([1])}))
    assert exc.value.code == "CDB-6130"


# ---------------------------------------------------------------------------
# Python UDFs
# ---------------------------------------------------------------------------


def test_python_udf_decorator_minimal_form() -> None:
    @udf
    def add_one(x: pa.Array) -> pa.Array:
        return pa.compute.add(x, 1)

    out = add_one(pa.array([1, 2, 3]))
    assert out.to_pylist() == [2, 3, 4]


def test_python_udf_validates_return_type() -> None:
    @udf(returns=pa.float32())
    def half(x):
        return pa.array([float(v) / 2 for v in x.to_pylist()], type=pa.float32())

    out = half(pa.array([2, 4, 6]))
    assert out.to_pylist() == [1.0, 2.0, 3.0]


def test_python_udf_arg_type_mismatch_raises() -> None:
    @udf(arg_types=(pa.string(),))
    def length(x):
        return pa.compute.utf8_length(x)

    with pytest.raises(CaracalError) as exc:
        length(pa.array([1, 2]))
    assert exc.value.code == "CDB-6131"


def test_udf_registry_register_and_call() -> None:
    reg = UdfRegistry()
    reg.register(PyUdf(name="square", fn=lambda x: pa.compute.multiply(x, x)))
    out = reg.call("square", pa.array([2, 3]))
    assert out.to_pylist() == [4, 9]


# ---------------------------------------------------------------------------
# Procedures
# ---------------------------------------------------------------------------


def test_procedure_if_assign_for_runs() -> None:
    proc = Procedure(
        name="bump",
        params=("seed",),
        steps=(
            AssignStep(var="total", expr=lambda c: 0),
            IfStep(
                predicate=lambda c: c.variables["seed"] > 0,
                then_steps=(
                    ForStep(
                        var="i",
                        iterable=lambda c: range(c.variables["seed"]),
                        body=(
                            AssignStep(
                                var="total",
                                expr=lambda c: c.variables["total"] + c.variables["i"],
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )
    ctx = proc.run({"seed": 4})
    # 0 + 1 + 2 + 3 = 6
    assert ctx.variables["total"] == 6


def test_procedure_while_max_iterations_guards_runaway() -> None:
    proc = Procedure(
        name="forever",
        params=(),
        steps=(WhileStep(predicate=lambda c: True, body=(), max_iterations=5),),
    )
    with pytest.raises(CaracalError) as exc:
        proc.run()
    assert exc.value.code == "CDB-6141"


def test_procedure_unknown_arg_rejected() -> None:
    proc = Procedure(name="p", params=("x",), steps=(CallStep(fn=lambda c: None),))
    with pytest.raises(CaracalError) as exc:
        proc.run({"y": 1})
    assert exc.value.code == "CDB-6140"
