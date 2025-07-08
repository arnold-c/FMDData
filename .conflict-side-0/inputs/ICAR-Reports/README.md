```bash
shell> tree
.
├── extracted-seroprevalence-tables
│   ├── 2019_Annual-Report_A-N-Islands.csv
│   ├── 2019_Annual-Report_Andhra-Pradesh.csv
│   ├── 2019_Annual-Report_Bihar.csv
│   ├── 2019_Annual-Report_Chhattisgarh.csv
│   ├── 2019_Annual-Report_Goa.csv
│   ├── 2019_Annual-Report_Gujarat.csv
│   ├── 2019_Annual-Report_Haryana.csv
│   ├── 2019_Annual-Report_Jammu-Kashmir.csv
│   ├── 2019_Annual-Report_Karnataka.csv
│   ├── 2019_Annual-Report_Kerala.csv
│   ├── 2019_Annual-Report_Madhya-Pradesh.csv
│   ├── 2019_Annual-Report_Maharashtra.csv
│   ├── 2019_Annual-Report_Manipur.csv
│   ├── 2019_Annual-Report_Mizoram.csv
│   ├── 2019_Annual-Report_Odisha.csv
│   ├── 2019_Annual-Report_Pondicherry.csv
│   ├── 2019_Annual-Report_Punjab.csv
│   ├── 2019_Annual-Report_Rajasthan.csv
│   ├── 2019_Annual-Report_Tamil-Nadu.csv
│   ├── 2019_Annual-Report_Telangana.csv
│   ├── 2019_Annual-Report_Uttar-Pradesh.csv
│   ├── 2019_Annual-Report_Uttarakhand.csv
│   ├── 2019_Annual-Report_West-Bengal.csv
│   ├── 2020_Annual-Report_NADCP-1.csv
│   ├── 2020_Annual-Report_Organized-farms.csv
│   ├── 2021_Annual-Report_NADCP-1.csv
│   ├── 2021_Annual-Report_NADCP-2.csv
│   ├── 2021_Annual-Report_Organized-farms.csv
│   ├── 2022_Annual-Report_NADCP-2.csv
│   ├── 2022_Annual-Report_NADCP-3.csv
│   ├── 2022_Annual-Report_Organized-farms.csv
│   └── README.md
├── pdfs
│   ├── 2015_Annual-Report.pdf
│   ├── 2016_Annual-Report.pdf
│   ├── 2017_Annual-Report.pdf
│   ├── 2018_Annual-Report.pdf
│   ├── 2019_Annual-Report.pdf
│   ├── 2020_Annual-Report.pdf
│   ├── 2021_Annual-Report.pdf
│   └── 2022_Annual-Report.pdf
└── README.md

3 directories, 42 files
```

- `extracted-seroprevalence-tables/` contains seroprevalence tables from the annual reports, extracted and saved in separate CSV files.
    - `README.md` contains additional information about the extracted tables, e.g., report page number, table/figure numbers in the original report, summaries about the states included, information about the diagnostic test used, general information about the program and sampling plans etc.
    - It is important to not that not every state appears in each year's report.
- `pdfs/` contains the underlying ICAR Annual Reports in PDF format used to extract pertinent tables from.
