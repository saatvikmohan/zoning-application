import os
from textextract import start_document_text_detection, get_document_text_detection, extract_text_from_responses_with_blocks

def process_pdf_to_text(input_path, output_path):
    """Process a PDF file: extract text and save to the current directory"""
    try:
        with open(input_path, 'rb') as document:
            job_id = start_document_text_detection(document)
            if job_id:
                responses = get_document_text_detection(job_id)
                extracted_sections = extract_text_from_responses_with_blocks(responses)
                
                # Save full text
                full_text = "\n".join(extracted_sections)
                text_file_path = os.path.join(output_path, os.path.basename(input_path).replace('.pdf', '.txt'))
                with open(text_file_path, 'w') as text_file:
                    text_file.write(full_text)
                
                print(f"Extracted text saved to {text_file_path}")
            else:
                print("Failed to start text detection job")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    input_file = 'staff-data-single-item.pdf'
    output_path = '.'
    
    if os.path.exists(input_file):
        process_pdf_to_text(input_file, output_path)
    else:
        print(f"Input file {input_file} does not exist.")