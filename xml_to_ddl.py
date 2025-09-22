import os
import glob
import xml.etree.ElementTree as ET

def extract_columns(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    results = []

    for elem in root.iter("column"):
        col_name = elem.get("originalDbColumnName", "")

        talend_type = elem.get("talendType", "")
        # Remove "id_" prefix if present
        if talend_type.startswith("id_"):
            talend_type = talend_type[3:]

        talend_type = talend_type.lower()
        pattern = elem.get("pattern", "")
        length = elem.get("length", "")
        precision = elem.get("precision", "")

        # Special conversion for bigdecimal → double
        if talend_type == "bigdecimal":
            talend_type = "decimal"

        # Decide how to format type
        if talend_type in ("string", "varchar"):
            final_type = talend_type
        elif talend_type == "date":
            final_type = talend_type
        elif length or precision:
            final_type = f"{talend_type}({length},{precision})"
        else:
            final_type = talend_type

        results.append(f"{col_name.lower()}|{final_type}|{pattern}")

    return results


if __name__ == "__main__":
    input_base_folder = "xml"        # base folder with subfolders
    output_base_folder = "xml_ddl_output"

    for root_dir, _, files in os.walk(input_base_folder):
        for file_name in files:
            if file_name.endswith(".xml"):
                xml_file = os.path.join(root_dir, file_name)
                rows = extract_columns(xml_file)

                # Preserve folder structure in output
                relative_path = os.path.relpath(root_dir, input_base_folder)
                output_folder = os.path.join(output_base_folder, relative_path)
                os.makedirs(output_folder, exist_ok=True)

                # Output file path
                base_name = os.path.splitext(file_name)[0]
                output_file = os.path.join(output_folder, f"{base_name}.sql")

                with open(output_file, "w", encoding="utf-8") as f:
                    f.write("originalDbColumnName,talendType,pattern\n")
                    for row in rows:
                        f.write(row + "\n")

                print(f"✅ Processed {xml_file} → {output_file}")
