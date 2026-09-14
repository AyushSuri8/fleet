"""Job handlers — same contract as the old fleet:
    handler(payload: dict, job: dict, ctx: dict) -> dict
ctx["checkpoint"](dict) persists resumable progress on the MANAGER, so jobs
survive node rotation and the 12h session cap."""
import time


def handle_echo(payload, job, ctx):
    return {"echo": payload, "processedBy": ctx["node_id"]}


def handle_checkpoint_demo(payload, job, ctx):
    steps = int(payload.get("steps", 10))
    if steps < 0 or steps > 1000:
        raise ValueError(f"steps must be 0..1000, got {steps}")
    start = int((job.get("checkpoint") or {}).get("cursor", 0))
    for cursor in range(start, steps):
        time.sleep(0.5)
        ctx["checkpoint"]({"cursor": cursor + 1, "stage": "processing"})
    return {"steps": steps, "done": True}


HANDLERS = {"echo": handle_echo, "checkpoint_demo": handle_checkpoint_demo}


def get_handler(job_type):
    return HANDLERS.get(job_type)
