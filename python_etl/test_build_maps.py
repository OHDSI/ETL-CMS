#!/usr/bin/env python3
"""Regression tests for build_maps() in CMS_SynPuf_ETL_CDM_v5.py.

Covers OHDSI/ETL-CMS issue #76 ("key error '40213148'"): build_maps() crashed
with a bare KeyError when a "Maps to" relationship target concept had no entry
in domain_dict (e.g. its CONCEPT.csv record contained an embedded newline and
was silently skipped while building domain_dict, or plain vocabulary skew).
The whole ETL run died instead of logging the problem and skipping that one
mapping.

NOTE ON PYTHON VERSIONS: the ETL itself is written for Python 2.7, which is not
available in every environment. These tests therefore apply a small, purely
mechanical source-to-source translation (``print x`` -> ``print(x)``,
``d.has_key(k)`` -> ``k in d``, ``.iteritems()`` -> ``.items()``) to the real
module file and import the result under Python 3. The translation is identical
whether the module is pristine or fixed, so the only behavioural difference
under test is the fix itself.
"""

import importlib.util
import os
import re
import sys
import tempfile
import types
import unittest

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
ETL_MODULE = os.path.join(THIS_DIR, "CMS_SynPuf_ETL_CDM_v5.py")


def _py2_to_py3(src):
    """Mechanical Python 2 -> 3 translation for the constructs used by the ETL."""
    # normalize indentation: the file mixes tabs and spaces, which Python 2
    # tolerated and Python 3 rejects (TabError). Expand leading whitespace
    # with tabsize 8, matching Python 2's indentation semantics. Only the
    # leading run is touched so tabs inside string literals are preserved.
    norm = []
    for line in src.splitlines():
        stripped = line.lstrip(" \t")
        indent = line[:len(line) - len(stripped)]
        norm.append(indent.expandtabs(8) + stripped)
    src = "\n".join(norm)
    # date(2009,01,01) -> date(2009,1,1): Python 2 allowed leading-zero int
    # literals; Python 3 does not. Scoped to date(...) calls only so that
    # meaningful leading zeros inside string literals (state codes '01',
    # date strings '2008-01-01') are never touched.
    src = re.sub(r"\bdate\((\d+),0+(\d+),0+(\d+)\)",
                 r"date(\1,\2,\3)", src)
    # print <expr>  ->  print(<expr>)   (statement position or after `:`)
    def _fix_print(m):
        return "%sprint(%s)" % (m.group(1), m.group(2).rstrip())
    src = re.sub(r"^([ \t]*)print ([^#\n]*?)[ \t]*$", _fix_print, src, flags=re.M)
    src = re.sub(r"(:[ \t]*)print ([^#\n]*?)[ \t]*$", _fix_print, src, flags=re.M)
    # d.has_key(k)  ->  k in d   (also correct under a leading `not`)
    src = re.sub(r"(\S+?)\.has_key\(((?:[^()]|\([^()]*\))*)\)", r"\2 in \1", src)
    # .iteritems()  ->  .items()
    src = src.replace(".iteritems()", ".items()")
    return src


def load_etl(tmpdir):
    """Import the (translated) ETL module with stubbed heavy deps."""
    for name in ("dotenv", "beneficiary", "FileControl", "SynPufFiles"):
        stub = types.ModuleType(name)
        if name == "dotenv":
            stub.load_dotenv = lambda *a, **k: None
        elif name == "beneficiary":
            stub.Beneficiary = type("Beneficiary", (), {})
        elif name == "FileControl":
            stub.FileControl = type("FileControl", (), {})
        elif name == "SynPufFiles":
            for cls in ("PrescriptionDrug", "InpatientClaim", "OutpatientClaim",
                        "CarrierClaim"):
                setattr(stub, cls, type(cls, (), {}))
        sys.modules[name] = stub
    for var in ("BASE_ETL_CONTROL_DIRECTORY", "BASE_SYNPUF_INPUT_DIRECTORY",
                "BASE_OMOP_INPUT_DIRECTORY", "BASE_OUTPUT_DIRECTORY",
                "SYNPUF_DIR_FORMAT"):
        os.environ[var] = tmpdir

    with open(ETL_MODULE, "r") as fh:
        src = _py2_to_py3(fh.read())
    # sanity: no py2-only syntax left behind
    assert "has_key" not in src, "transpile missed a has_key() call"
    for i, line in enumerate(src.splitlines(), 1):
        if line.lstrip().startswith("#"):
            continue
        assert not re.search(r"(^|[;:{}\s])print [^(\s]", line), \
            "transpile missed a print statement at line %d" % i

    mod_path = os.path.join(tmpdir, "etl_under_test.py")
    with open(mod_path, "w") as fh:
        fh.write(src)
    spec = importlib.util.spec_from_file_location("etl_under_test", mod_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["etl_under_test"] = mod
    spec.loader.exec_module(mod)
    mod.current_stats_filename = os.path.join(tmpdir, "etl_stats.txt")
    return mod


CONCEPT_HEADER = ("concept_id\tconcept_name\tdomain_id\tvocabulary_id\t"
                  "concept_class_id\tstandard_concept\tconcept_code\t"
                  "valid_start_date\tvalid_end_date\tinvalid_reason\n")
REL_HEADER = ("concept_id_1\tconcept_id_2\trelationship_id\t"
              "valid_start_date\tvalid_end_date\tinvalid_reason\n")


def write_fixtures(tmpdir, concept_rows, rel_rows):
    with open(os.path.join(tmpdir, "CONCEPT.csv"), "w") as fh:
        fh.write(CONCEPT_HEADER)
        fh.writelines(concept_rows)
    with open(os.path.join(tmpdir, "CONCEPT_RELATIONSHIP.csv"), "w") as fh:
        fh.write(REL_HEADER)
        fh.writelines(rel_rows)


def concept_row(cid, name, domain, vocab, code):
    return "%s\t%s\t%s\t%s\tClinical Finding\t\t%s\t20000101\t20991231\t\n" % (
        cid, name, domain, vocab, code)


class BuildMapsTests(unittest.TestCase):
    def test_maps_to_target_missing_from_domain_dict_does_not_crash(self):
        """Issue #76: Maps-to target whose CONCEPT.csv record has an embedded
        newline is skipped while building domain_dict; build_maps() must log
        and skip that mapping instead of dying with KeyError."""
        tmpdir = tempfile.mkdtemp()
        write_fixtures(
            tmpdir,
            [concept_row("1", "Acute myocardial infarction", "Condition",
                         "ICD9CM", "41090"),
             # embedded newline -> split across two physical lines, each with
             # the wrong field count, so pass 1 silently drops it from
             # domain_dict (this is the documented hazard in build_maps)
             ("40213148\tHeart failure\n"
              "with exacerbation\tCondition\tSNOMED\tClinical Finding\t"
              "S\t10000\t20000101\t20991231\t\n")],
            ["1\t40213148\tMaps to\t20000101\t20991231\t\n"])
        mod = load_etl(tmpdir)

        mod.build_maps()  # must not raise KeyError (issue #76)

        key = ("ICD9", "41090")
        self.assertIn(key, mod.source_code_concept_dict)
        # mapping skipped: no destination file could be resolved
        self.assertEqual(mod.source_code_concept_dict[key], [])
        with open(os.path.join(tmpdir, "concept_debug_log.txt")) as fh:
            log = fh.read()
        self.assertIn("No domain found for mapped concept 40213148", log)

    def test_maps_to_valid_target_still_maps(self):
        """Happy path guard: a well-formed Maps-to target still resolves to
        its destination file exactly as before."""
        tmpdir = tempfile.mkdtemp()
        write_fixtures(
            tmpdir,
            [concept_row("2", "Aspirin 100mg", "Drug", "NDC", "12345"),
             concept_row("201820", "Aspirin", "Drug", "RxNorm", "1191")],
            ["2\t201820\tMaps to\t20000101\t20991231\t\n"])
        mod = load_etl(tmpdir)

        mod.build_maps()

        key = ("NDC", "12345")
        mapped = mod.source_code_concept_dict[key]
        self.assertEqual(len(mapped), 1)
        self.assertEqual(mapped[0].target_concept_id, "201820")
        self.assertEqual(mapped[0].destination_file, "drug")


if __name__ == "__main__":
    unittest.main(verbosity=2)
