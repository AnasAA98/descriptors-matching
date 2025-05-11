# main.py
import re
import pandas as pd
from rapidfuzz import fuzz, process, utils
from sqlalchemy import create_engine, text
import config


# Helpers
def build_engine():
    url = (
        f"mysql+mysqlconnector://{config.DB_USER}:{config.DB_PASS}"
        f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )
    return create_engine(url, echo=False, future=True)


# --- cleaning regexes
_punct   = re.compile(r"[^\w\s]")
_date_rx = re.compile(r"\b\d{2}/\d{2}\b")          
_store   = re.compile(r"\b\d{3,}\b")               
_order   = re.compile(r"ORDER\s*#?\s*\d+", re.I)   

def clean_descriptor(raw: str) -> str:
    core = raw.split("*", 1)[0]
    core = _date_rx.sub(" ", core)
    core = _store.sub(" ", core)
    core = _order.sub(" ", core)
    core = _punct.sub(" ", core)
    return " ".join(core.upper().split())


def load_merchant_lookup(conn):
    rows = conn.execute(text(
        "SELECT merchant_id, merchant_name FROM merchant_list"
    ))
    mapping = {r.merchant_name.upper(): r.merchant_id for r in rows}
    return mapping, list(mapping.keys())


def load_aliases():
    try:
        df = pd.read_csv("aliases.csv")
        return {p.upper(): c.upper() for p, c in zip(df.pattern, df.canonical)}
    except FileNotFoundError:
        return {}


tokenize = lambda s: utils.default_process(s).split()
def shared_tokens(desc_tokens, merch):
    return len(set(desc_tokens) & set(tokenize(merch)))


# Main workflow
def main():
    engine = build_engine()

    with engine.begin() as conn:
        for col_sql in (
            "ADD COLUMN cleaned_descriptor TEXT NULL AFTER descriptor",
            "ADD COLUMN merchant_name      VARCHAR(255) NULL",
            "ADD COLUMN merchant_id        VARCHAR(36) NULL",
        ):
            try:
                conn.execute(text(f"ALTER TABLE descriptors {col_sql};"))
            except Exception as e:
                if "1060" not in str(e):  
                    raise

    aliases = load_aliases()

    # ---- load + clean 
    with engine.begin() as conn:
        df = pd.read_sql("SELECT descriptor_id, descriptor FROM descriptors", conn)
        df["cleaned"] = df["descriptor"].apply(clean_descriptor)

        # alias prefix substitution
        def alias_sub(s):
            for patt, canon in aliases.items():
                if s.startswith(patt):
                    return canon
            return s
        df["cleaned"] = df["cleaned"].apply(alias_sub)

        name_to_id, name_list = load_merchant_lookup(conn)

        matches, no_matches = [], []

        for _, row in df.iterrows():
            did, raw, cleaned = row["descriptor_id"], row["descriptor"], row["cleaned"]

            # exact / substring
            exact = next((nm for nm in name_list if nm in cleaned), None)
            if exact:
                matches.append((did, cleaned, exact, name_to_id[exact]))
                continue

            
            d_tokens = tokenize(cleaned)
            pool = [m for m in name_list if shared_tokens(d_tokens, m) > 0] or name_list

            #  RapidFuzz scores
            best, best_score, _ = process.extractOne(cleaned, pool, scorer=fuzz.token_set_ratio)
            alt,  alt_score,  _ = process.extractOne(cleaned, pool, scorer=fuzz.partial_ratio)
            if alt_score > best_score:
                best, best_score = alt, alt_score

            # second best for gap
            top2 = process.extract(cleaned, pool, scorer=fuzz.token_set_ratio, limit=2)
            second = top2[1][1] if len(top2) > 1 else 0
            gap = best_score - second
            token_hit = best in cleaned

            # acceptance
            if best_score >= 85 \
               or (token_hit and best_score >= 70) \
               or (best_score >= 70 and gap >= 15):
                matches.append((did, cleaned, best, name_to_id.get(best)))
            else:
                no_matches.append((did, raw, cleaned, best, best_score))

        upd = text("""
            UPDATE descriptors
               SET cleaned_descriptor = :cleaned,
                   merchant_name      = :mname,
                   merchant_id        = :mid
             WHERE descriptor_id     = :did
        """)
        for did, cleaned, mname, mid in matches:
            conn.execute(upd, {
                "cleaned": cleaned,
                "mname":   mname.title(),
                "mid":     mid,
                "did":     did,
            })

    total = len(df)
    print(f"Matched {len(matches):,} / {total:,} rows "
          f"({total - len(matches)} unmatched)")

    if no_matches:
        pd.DataFrame(
            no_matches,
            columns=["descriptor_id", "raw_descriptor",
                     "cleaned_descriptor", "best_guess", "score"]
        ).to_csv("unmatched.csv", index=False)
        print("⚠️  Unmatched descriptors written to unmatched.csv")


if __name__ == "__main__":
    main()
