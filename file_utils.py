import json
import csv

def write_json(file_path, content):
    """
    Writes the given content to a JSON file.

    :param file_path: Path to the file
    :param content: Content (list/dict) to write to the file
    """
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(content, file, ensure_ascii=False, indent=4)
        print(f"JSON data written to {file_path}")
    except Exception as e:
        print(f"Error writing JSON file: {e}")

def write_tsv(file_path, content):
    """
    Writes the given content to a TSV file.

    :param file_path: Path to the file
    :param content: List of dictionaries to write as TSV
    """
    try:
        if not content:
            print("No data to write.")
            return
        
        # Extract column names from the first dictionary
        fieldnames = content[0].keys()
        
        with open(file_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter="\t")
            writer.writeheader()
            writer.writerows(content)
        
        print(f"TSV data written to {file_path}")
    except Exception as e:
        print(f"Error writing TSV file: {e}")


def read_file(file_path):
    """
    Reads the content of a file and returns it.

    :param file_path: Path to the file
    :return: Content of the file
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        return content
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

def read_tsv(file_path):
    try:
        with open(file_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            return list(reader)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []
    except Exception as e:
        print(f"Error reading TSV file: {e}")
        return []

all_rows = read_tsv("all_rows.txt")


def append_to_file(file_path, content):
    """
    Appends the given content to a file.

    :param file_path: Path to the file
    :param content: Content to append to the file
    """
    write_to_file(file_path, content, mode='a')
