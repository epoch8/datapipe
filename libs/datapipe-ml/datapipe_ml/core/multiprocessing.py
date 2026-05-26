import multiprocessing as mp


def _spawn(target, *args):
    # https://github.com/pytorch/pytorch/issues/3492#issuecomment-522393847
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    p = ctx.Process(target=target, args=(q, *args))
    p.start()
    try:
        res = q.get()
        p.join()
        return res
    finally:
        if p.is_alive():
            p.join()
