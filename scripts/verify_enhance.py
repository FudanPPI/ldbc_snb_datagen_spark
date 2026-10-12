#!/usr/bin/env python3
"""Combined structural validation for skew-enhanced datagen output.

Checks the combined enhance pipeline (F1..F6) for structural integrity and the
presence of every enabled feature. Per-module exact replays live in the F2..F6
commit history (see docs/ENHANCE.md); this script is the fast post-run gate.

Usage:
  python3 scripts/verify_enhance.py <enhanced-output-dir> [native-output-dir]

Both dirs must contain graphs/csv/raw/composite-merged-fk/... The native dir is
optional; when given, row-count comparisons are made against it.
"""
import csv
import glob
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

NUMPERSONS = 10620   # SF1; adjust when validating other scale factors
K = 20               # enhance.remap.hotEvery
M = (NUMPERSONS + K - 1) // K
RESERVE254 = 254 << 36                  # F3 reply band
DANG253 = 253 << 36                     # F6 dangling band [253<<36, 254<<36)
SMALLCOMP = 1000                        # enhance.smallcomp.count
RMAT = 100000                           # enhance.rmat.nodes
RECENT_SPAN = 30 * 86400000
WINDOW_MILLIS = 3600000
WINDOW_COUNT = 24
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MS = timedelta(milliseconds=1)


def l(x):
    return None if x is None or x == "" else int(x)


def ts(s):
    return None if not s else (datetime.fromisoformat(s) - EPOCH) // MS


def read_table(root, sub, name):
    rows = []
    for f in sorted(glob.glob(os.path.join(root, "graphs/csv/raw/composite-merged-fk", sub, name, "part-*.csv"))):
        with open(f, newline="") as fh:
            rows.extend(csv.DictReader(fh, delimiter="|"))
    return rows


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    enh = sys.argv[1].rstrip("/")
    base = sys.argv[2].rstrip("/") if len(sys.argv) > 2 else None
    ok = True

    def check(cond, msg):
        nonlocal ok
        print(("PASS  " if cond else "FAIL  ") + msg)
        if not cond:
            ok = False

    person = read_table(enh, "dynamic", "Person")
    post = read_table(enh, "dynamic", "Post")
    com = read_table(enh, "dynamic", "Comment")
    knows = read_table(enh, "dynamic", "Person_knows_Person")
    likes_p = read_table(enh, "dynamic", "Person_likes_Post")
    likes_c = read_table(enh, "dynamic", "Person_likes_Comment")

    person_ids = [l(r["id"]) for r in person]
    pid_set = set(person_ids)
    post_ids = {l(r["id"]) for r in post}
    com_ids = {l(r["id"]) for r in com}
    print(f"rows: person={len(person)} post={len(post)} comment={len(com)} knows={len(knows)} "
          f"likesP={len(likes_p)} likesC={len(likes_c)}")

    reserve = [p for p in person_ids
               if (255 << 36) + NUMPERSONS <= p < (255 << 36) + NUMPERSONS + SMALLCOMP + RMAT]
    hot_deg = Counter()
    for r in knows:
        hot_deg[l(r["Person1Id"])] += 1
        hot_deg[l(r["Person2Id"])] += 1
    max_hot = max(hot_deg.get(p, 0) for p in range(M))

    p_end0 = max(ts(r["deletionDate"]) for r in post)
    win_starts = [(p_end0 - RECENT_SPAN - 86400000) + i * (RECENT_SPAN // WINDOW_COUNT)
                  for i in range(WINDOW_COUNT)]
    n_burst = sum(1 for r in post
                  if any(s <= ts(r["creationDate"]) < s + WINDOW_MILLIS + 86400000 for s in win_starts))
    n_null = sum(1 for r in post if not r["content"])
    n_badip = sum(1 for r in post if r["locationIP"] == "999.999.999.999")
    n_baddate = sum(1 for r in post if ts(r["creationDate"]) == 0)
    n_loop = sum(1 for r in knows if l(r["Person1Id"]) == l(r["Person2Id"]))
    dup_edges = Counter((l(r["Person1Id"]), l(r["Person2Id"])) for r in knows)
    n_dup = sum(c - 1 for c in dup_edges.values() if c > 1)
    n_dangle_p = sum(1 for r in likes_p if DANG253 <= l(r["PostId"]) < RESERVE254)
    n_dangle_c = sum(1 for r in likes_c if DANG253 <= l(r["CommentId"]) < RESERVE254)
    bc = Counter(r["browserUsed"] for r in post)
    top_share = bc.most_common(1)[0][1] / len(post)

    miss_creator = sum(1 for r in post if l(r["CreatorPersonId"]) not in pid_set) + \
        sum(1 for r in com if l(r["CreatorPersonId"]) not in pid_set)
    clean_knows_bad = sum(1 for r in knows
                          if l(r["Person1Id"]) != l(r["Person2Id"]) and
                          (l(r["Person1Id"]) not in pid_set or l(r["Person2Id"]) not in pid_set))
    clean_lp = sum(1 for r in likes_p
                   if not (DANG253 <= l(r["PostId"]) < RESERVE254) and
                   (l(r["PostId"]) not in post_ids or l(r["PersonId"]) not in pid_set))
    clean_lc = sum(1 for r in likes_c
                   if not (DANG253 <= l(r["CommentId"]) < RESERVE254) and
                   (l(r["CommentId"]) not in com_ids or l(r["PersonId"]) not in pid_set))

    check(len(reserve) == SMALLCOMP + RMAT, f"reserve band holds smallcomp+rmat persons ({len(reserve)})")
    check(max_hot >= 100, f"supernode degree skew present (max hot-band degree={max_hot})")
    check(len([p for p in com_ids if RESERVE254 <= p < (255 << 36)]) > 0, "hot-message reply comments present")
    check(n_burst > 0, f"burst-window posts present ({n_burst})")
    check(n_null > 0, f"null/empty content present ({n_null})")
    check(n_badip > 0, f"invalid IP present ({n_badip})")
    check(n_baddate > 0, f"out-of-range creationDate present ({n_baddate})")
    check(n_loop > 0, f"self-loops present ({n_loop})")
    check(n_dup > 0, f"duplicate edges present ({n_dup})")
    check(n_dangle_p > 0 and n_dangle_c > 0,
          f"dangling likes present (post={n_dangle_p} comment={n_dangle_c})")
    check(top_share > 0.2, f"zipf browser skew (top share={top_share:.3f})")
    check(miss_creator == 0, f"Post/Comment creator FKs intact ({miss_creator} dangling)")
    check(clean_knows_bad == 0, f"knows endpoints intact on non-loop edges ({clean_knows_bad})")
    check(clean_lp == 0, f"likes-post FKs intact on non-dangling rows ({clean_lp})")
    check(clean_lc == 0, f"likes-comment FKs intact on non-dangling rows ({clean_lc})")

    if base:
        b_person = read_table(base, "dynamic", "Person")
        b_post = read_table(base, "dynamic", "Post")
        b_com = read_table(base, "dynamic", "Comment")
        check(len(person) == len(b_person) + SMALLCOMP + RMAT,
              f"person = native+smallcomp+rmat ({len(person)} vs {len(b_person)})")
        check(len(post) == len(b_post), f"Post row count unchanged vs native ({len(post)})")
        check(len(com) > len(b_com), f"Comment grew vs native (replies) ({len(com)} > {len(b_com)})")

    print("\nRESULT:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
