# Member Eligibility

This project calculates and reports data about providers based on member eligibility and membership months.

## Requirements

- **Language:** Scala
- **Framework:** Apache Spark
- **Output:** JSON files
- **IDE:** Open

## Data

The datasets used in this project are:

1. `member_eligibility.csv` - A CSV containing data about members and their respective personal details.
2. `member_months.csv` - A CSV with the unique member number, the member ID, and the date of eligibility with the health plan.

## Problem Statement

The project performs the following tasks:

1. **Calculate the total number of member months:**
    - The result contains the member's ID, full name, and the total number of member months.
    - The output is saved in JSON format, partitioned by the member ID.

2. **Calculate the total number of member months per member per year:**
    - The result contains the member's ID, the year, and the total number of member months for that year.
    - The output is saved in JSON format.

## Project Structure

