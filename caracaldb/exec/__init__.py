"""Execution engine — pull-based Arrow operators."""

from caracaldb.exec.operator import ExecCtx, PhysicalOperator, run_pipeline

__all__ = ["ExecCtx", "PhysicalOperator", "run_pipeline"]
