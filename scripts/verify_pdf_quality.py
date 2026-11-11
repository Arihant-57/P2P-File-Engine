#!/usr/bin/env python3
import os
import pymupdf

pdf_path = os.path.abspath("C:/Users/ariha/Documents/Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine/dist/P2P_File_Engine_Complete_Project_Guide.pdf")

doc = pymupdf.open(pdf_path)
print("=== PDF INSPECTION REPORT ===")
print(f"File: {pdf_path}")
print(f"Total Pages: {len(doc)}")
print(f"Title: {doc.metadata.get('title')}")
print(f"Author: {doc.metadata.get('author')}")
print(f"Subject: {doc.metadata.get('subject')}")

toc = doc.get_toc()
print(f"Bookmarks / TOC Entries: {len(toc)}")
for item in toc:
    indent = "  " * item[0]
    print(f"{indent}- [{item[0]}] {item[1]} (Page {item[2]})")

full_text = "\n".join([f"--- Page {i+1} ---\n" + p.get_text() for i, p in enumerate(doc)])

required_sections = [
    "Project Introduction and Theoretical Foundations",
    "Computer Science Syllabus Mapping",
    "Object-Oriented Programming (OOP)",
    "Data Structures and Algorithms (DSA)",
    "Operating Systems & Systems Programming",
    "Computer Networks",
    "Database Management Systems (DBMS)",
    "Cryptography and Data Integrity",
    "Software Engineering & Verification",
    "System Architecture and Mesh Topology",
    "Peer Discovery, Wire Protocol, and Network Lifecycle",
    "Complete File Upload Workflow",
    "Reed-Solomon RS(3,2) Erasure Coding Mathematics",
    "Distributed Metadata Management and SQLite Schema",
    "File Search, Download, and Reconstruction Workflow",
    "Autonomous Failure Detection and Durability Recovery",
    "Embedded Web Dashboard and REST API Architecture",
    "Complete Source Code Walkthrough",
    "Installation, Deployment, and User Guide",
    "Markdown and Project File-Type Reference",
    "Comprehensive Testing and Verification Report",
    "Four-Laptop Demonstration Script & Faculty Evaluation Guide",
    "Security Analysis and Engineering Limitations",
    "Glossary of Technical Terms & Educational Outcomes",
    "Official Artifacts and Cryptographic Checksums"
]

print("\n=== SECTION COMPLETENESS VERIFICATION ===")
all_found = True
for sec in required_sections:
    found = sec.lower() in full_text.lower()
    status = "[PASS]" if found else "[FAIL]"
    print(f"{status} {sec}")
    if not found:
        all_found = False

if all_found:
    print("\nALL REQUIRED SECTIONS ARE FULLY PRESENT AND SEARCHABLE IN THE PDF!")
else:
    print("\nWARNING: Some sections were missing from PDF text.")

doc.close()
