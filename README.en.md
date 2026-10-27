**[中文](README.md) | English**

# Mapping-

A crawler toolkit that helps HR automate talent mapping. It collects student information from target laboratory websites and produces structured lists in batches, reducing repetitive manual research.

## Covered Sources

| Script | Source |
|---|---|
| `scrape_madsys_students.py` | Tsinghua University MADSys Laboratory (madsys.cs.tsinghua.edu.cn) |
| `scrape_storage_students.py` | Tsinghua University Storage Research Group (storage.cs.tsinghua.edu.cn) |

## Output Fields

Name, student type (PhD/master's), laboratory, personal bio, education, awards, teaching experience, and email, plus automatically retrieved Google Scholar / GitHub / LinkedIn / personal homepage links.

The scripts filter search-engine and university-domain results and identify competitor-school markers. Results can be exported to CSV or Excel with pandas.

## Installation & Usage

```bash
pip install pandas requests beautifulsoup4
python3 scrape_madsys_students.py -o madsys.csv
python3 scrape_storage_students.py -o storage.csv
```

The scripts use argparse and provide options such as output paths, request throttling, and school-alias mappings. Run the following command for all available options:

```bash
python3 scrape_madsys_students.py --help
```

## Data Use & Compliance

- Collect information only from public pages. Request throttling and an identifying User-Agent are built in.
- Follow each target site's robots policy and applicable university rules. Use the data only for lawful and compliant recruiting purposes.
