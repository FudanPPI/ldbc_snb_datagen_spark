#!/usr/bin/env python3
"""Spark-based structural validation for skew-enhanced datagen output.

Same check set as verify_enhance.py but implemented with Spark SQL
expressions so it scales to SF1000-class outputs (billions of rows)
that cannot be loaded row-by-row into Python memory.

Usage:
  spark-submit scripts/verify_enhance_spark.py [--quick] <enh-dir> [native-dir]

numPersons is derived automatically: the enhanced Person table holds the
101,000 reserve-band ids (smallcomp 1000 + rmat 100000) on top of the
native population, so NUMPERSONS = person_count - 101000. This works for
any scale factor.

--quick skips the two full anti-joins over likes-comment (the heaviest
check on SF1000: ~30 billion rows); everything else runs in full.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RESERVE255 = 255 << 36
RESERVE254 = 254 << 36
DANG253 = 253 << 36
RESERVE_TOTAL = 1000 + 100000        # smallcomp + rmat
RECENT_SPAN_MS = 30 * 86400000
WINDOW_MILLIS = 3600000
WINDOW_COUNT = 24


def main():
    args = [a for a in sys.argv[1:] if a != "--quick"]
    quick = "--quick" in sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(2)
    enh = args[0].rstrip("/")
    base = args[1].rstrip("/") if len(args) > 1 else None

    spark = (SparkSession.builder
             .appName("verify-enhance")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    ok = True

    def check(cond, msg):
        nonlocal ok
        print(("PASS  " if cond else "FAIL  ") + msg)
        if not cond:
            ok = False

    root = enh + "/graphs/csv/raw/composite-merged-fk"

    def table(sub, name):
        return (spark.read.option("header", True).option("sep", "|").csv(
            f"{root}/{sub}/{name}/part-*.csv"))

    def num(col):
        return F.col(col).cast("long")

    def iso(col):
        return F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

    person = table("dynamic", "Person").select(num("id").alias("id"))
    person.cache()
    n_person = person.count()
    NUMPERSONS = n_person - RESERVE_TOTAL
    M = (NUMPERSONS + 19) // 20
    print(f"rows: person={n_person} (native={NUMPERSONS}, hot band M={M})")

    post = table("dynamic", "Post")
    com = table("dynamic", "Comment")
    knows = table("dynamic", "Person_knows_Person")
    likes_p = table("dynamic", "Person_likes_Post")
    likes_c = table("dynamic", "Person_likes_Comment")

    def ren(t):
        for c, a in (("id", "id"), ("Person1Id", "p1"), ("Person2Id", "p2"),
                     ("CreatorPersonId", "creator"), ("PostId", "postid"),
                     ("CommentId", "commid"), ("PersonId", "personid"),
                     ("locationIP", "ip"), ("browserUsed", "browser"),
                     ("content", "content"), ("creationDate", "cd"),
                     ("deletionDate", "dd")):
            if c in t.columns:
                t = t.withColumnRenamed(c, a)
        return t

    post = ren(post)
    com = ren(com)
    knows = ren(knows)
    likes_p = ren(likes_p)
    likes_c = ren(likes_c)
    post = post.select(F.col("id").cast("long").alias("id"),
                       F.col("content"), F.col("ip"), F.col("browser"),
                       iso("cd").cast("long").alias("cd"),
                       iso("dd").cast("long").alias("dd"),
                       num("creator").alias("creator"))
    com = com.select(F.col("id").cast("long").alias("id"),
                     num("creator").alias("creator"))
    knows = knows.select(num("p1").alias("p1"), num("p2").alias("p2"))
    likes_p = likes_p.select(num("personid").alias("pid"),
                             num("postid").alias("fid"))
    likes_c = likes_c.select(num("personid").alias("pid"),
                             num("commid").alias("fid"))
    post.cache()
    com.cache()
    knows.cache()

    # --- feature traces -----------------------------------------------------
    reserve = person.where((F.col("id") >= RESERVE255 + NUMPERSONS) &
                           (F.col("id") < RESERVE255 + NUMPERSONS + RESERVE_TOTAL)).count()
    check(reserve == RESERVE_TOTAL,
          f"reserve band holds smallcomp+rmat persons ({reserve})")

    deg = (knows.where((F.col("p1") < M) | (F.col("p2") < M))
           .select(F.explode(F.array("p1", "p2")).alias("v"))
           .where(F.col("v") < M)
           .groupBy("v").count())
    max_hot = deg.select(F.max("count")).collect()[0][0] or 0
    check(max_hot >= 100, f"supernode degree skew present (max hot-band degree={max_hot})")

    n_reply = com.where((F.col("id") >= RESERVE254) &
                        (F.col("id") < RESERVE255)).count()
    check(n_reply > 0, f"hot-message reply comments present ({n_reply})")

    p_end0 = post.select(F.max("dd")).collect()[0][0]
    start = p_end0 - RECENT_SPAN_MS - 86400000
    step = RECENT_SPAN_MS // WINDOW_COUNT
    upper = start + (WINDOW_COUNT - 1) * step + WINDOW_MILLIS + 86400000
    off = F.col("cd") - start
    n_burst = post.where((F.col("cd") >= start) & (F.col("cd") < upper) &
                         (off - F.floor(off / step) * step <
                          WINDOW_MILLIS + 86400000)).count()
    check(n_burst > 0, f"burst-window posts present ({n_burst})")

    n_null = post.where(F.col("content").isNull() |
                        (F.col("content") == "")).count()
    check(n_null > 0, f"null/empty content present ({n_null})")
    n_badip = post.where(F.col("ip") == "999.999.999.999").count()
    check(n_badip > 0, f"invalid IP present ({n_badip})")
    n_baddate = post.where(F.col("cd") == 0).count()
    check(n_baddate > 0, f"out-of-range creationDate present ({n_baddate})")

    n_loop = knows.where(F.col("p1") == F.col("p2")).count()
    check(n_loop > 0, f"self-loops present ({n_loop})")
    n_dup = (knows.groupBy("p1", "p2").count().where(F.col("count") > 1)
             .select(F.sum(F.col("count") - 1)).collect()[0][0] or 0)
    check(n_dup > 0, f"duplicate edges present ({n_dup})")

    n_dangle_p = likes_p.where((F.col("fid") >= DANG253) &
                               (F.col("fid") < RESERVE254)).count()
    n_dangle_c = likes_c.where((F.col("fid") >= DANG253) &
                               (F.col("fid") < RESERVE254)).count()
    check(n_dangle_p > 0 and n_dangle_c > 0,
          f"dangling likes present (post={n_dangle_p} comment={n_dangle_c})")

    bc = post.groupBy("browser").count().collect()
    total = sum(r[1] for r in bc)
    top_share = max(r[1] for r in bc) / total
    check(top_share > 0.2, f"zipf browser skew (top share={top_share:.3f})")

    # --- referential integrity on non-dirty rows ----------------------------
    miss_creator = (post.where(F.col("creator").isNotNull())
                    .join(person, post.creator == person.id, "left_anti").count()
                    + com.where(F.col("creator").isNotNull())
                    .join(person, com.creator == person.id, "left_anti").count())
    check(miss_creator == 0, f"Post/Comment creator FKs intact ({miss_creator} dangling)")

    clean_knows_bad = (knows.where(F.col("p1") != F.col("p2"))
                       .join(person, F.col("p1") == person.id, "left_anti").count()
                       + knows.where(F.col("p1") != F.col("p2"))
                       .join(person, F.col("p2") == person.id, "left_anti").count())
    check(clean_knows_bad == 0,
          f"knows endpoints intact on non-loop edges ({clean_knows_bad})")

    clean_lp = (likes_p.where(~F.col("fid").between(DANG253, RESERVE254 - 1))
                .join(post, F.col("fid") == post.id, "left_anti").count()
                + likes_p.where(~F.col("fid").between(DANG253, RESERVE254 - 1))
                .join(person, F.col("pid") == person.id, "left_anti").count())
    check(clean_lp == 0,
          f"likes-post FKs intact on non-dangling rows ({clean_lp})")

    if quick:
        check(True, "likes-comment FKs skipped (--quick)")
    else:
        clean_lc = (likes_c.where(~F.col("fid").between(DANG253, RESERVE254 - 1))
                    .join(com, F.col("fid") == com.id, "left_anti").count()
                    + likes_c.where(~F.col("fid").between(DANG253, RESERVE254 - 1))
                    .join(person, F.col("pid") == person.id, "left_anti").count())
        check(clean_lc == 0,
              f"likes-comment FKs intact on non-dangling rows ({clean_lc})")

    if base:
        broot = base + "/graphs/csv/raw/composite-merged-fk"
        btab = lambda sub, name: spark.read.option("header", True).option("sep", "|").csv(
            f"{broot}/{sub}/{name}/part-*.csv")
        b_person = btab("dynamic", "Person").count()
        b_post = btab("dynamic", "Post").count()
        b_com = btab("dynamic", "Comment").count()
        check(n_person == b_person + RESERVE_TOTAL,
              f"person = native+smallcomp+rmat ({n_person} vs {b_person})")
        n_post = post.count()
        n_com = com.count()
        check(n_post == b_post, f"Post row count unchanged vs native ({n_post})")
        check(n_com > b_com, f"Comment grew vs native (replies) ({n_com} > {b_com})")

    print("\nRESULT:", "PASS" if ok else "FAIL")
    spark.stop()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
