"""Pluggable job handlers — THE place to add your real server logic once the
server type is decided (outline section 3). Map a job 'type' to a function.

Handler contract:
    def handle(payload: dict, job: dict, ctx: dict) -> dict  (result)

ctx["checkpoint"](dict)  -> persist resumable progress for this job
ctx["node_id"]           -> current node
"""
import logging
import time

log = logging.getLogger("handlers")


def handle_echo(payload, job, ctx):
    """Demo handler: minimal request/response sanity check."""
    return {"echo": payload, "processedBy": ctx["node_id"]}


def handle_checkpoint_demo(payload, job, ctx):
    """Demo handler: long-running, resumable from checkpoint (section 15)."""
    steps = int(payload.get("steps", 10))
    if steps < 0 or steps > 1000:
        raise ValueError(f"steps must be 0..1000, got {steps}")
    if len(str(payload)) > 32768:
        raise ValueError("payload too large")
    start = int((job.get("checkpoint") or {}).get("cursor", 0))
    for cursor in range(start, steps):
        time.sleep(0.5)  # pretend work
        ctx["checkpoint"]({"cursor": cursor + 1, "stage": "processing"})
    return {"steps": steps, "done": True}


HANDLERS = {
    "echo": handle_echo,
    "checkpoint_demo": handle_checkpoint_demo,
}


def get_handler(job_type):
    return HANDLERS.get(job_type)