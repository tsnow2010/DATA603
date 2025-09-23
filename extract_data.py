# If necessary, install pypdf package
# !pip install pypdf
# !pip install mrjob
# !pip install pyenchant

# Imports
from pypdf import PdfReader
from itertools import chain

#### Step 1: Load required text from PDF

# Load PDF
reader = PdfReader("harrypotter.pdf")

#### Step 2: Create file1.txt ####

# Define starting page
book_year_start = 2415 # Birth Month = June, start of Harry Potter and the Half-blood Prince
birth_day = 30 
start_page = book_year_start + birth_day - 1 # zero-indexed, hence the "-1"

# Define ending page
num_pages = 10 # includes starting page
end_page = start_page + num_pages

# Extract text
with open('file1.txt','w') as file:
    for page in reader.pages[start_page:end_page]:
        file.write(page.extract_text())

#### Step 3: Create file2.txt ####

# Define starting page
birth_year = 92
start_page = book_year_start + birth_year - 1 # zero-indexed, hence the "-1"

# Define ending page
num_pages = 10 # includes starting page
end_page = start_page + num_pages

# Extract text
with open('file2.txt','w') as file:
    for page in reader.pages[start_page:end_page]:
        file.write(page.extract_text())