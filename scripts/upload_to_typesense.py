import os
from dotenv import load_dotenv
import typesense

load_dotenv()

transcripts = os.listdir("acquired_transcripts")

ts_secret = os.environ.get("TS_ADMIN_SECRET")
ts_node = os.environ.get("TS_NODE")
ts_client = typesense.Client(
    {
        "nodes": [{"host": ts_node, "port": "443", "protocol": "https"}],
        "connection_timeout_seconds": 600,
        "api_key": ts_secret,
    }
)




def create_chunks():
    all_chunks = []
    for filename in transcripts:
        if filename.endswith(".txt"):
            doc_title = filename.split(".")[0].replace("_", " ")
            doc_text = open(f"acquired_transcripts/{filename}", "r").read()
            tokens = doc_text.split()
            chunks = []
            i = 0
            while i < len(tokens):
                end = i + 250
                if end < len(tokens):
                    # Look backwards for a logical end
                    logical_end = end
                    for j in range(end, max(i, end - 75), -1):
                        if tokens[j][-1] in {'.', '!', '?', '\n'}:
                            logical_end = j
                            break
                    # If no logical end found, look forward
                    if logical_end == end:
                        for j in range(end, min(len(tokens), end + 50)):
                            if tokens[j][-1] in {'.', '!', '?', '\n'}:
                                logical_end = j
                                break
                    # If still no logical end found, fallback to original method
                    if logical_end == end:
                        logical_end = i + 250
                else:
                    logical_end = len(tokens)
                chunk = f"{doc_title}\n\n{' '.join(tokens[i:logical_end])}"
                chunks.append(chunk)
                i = logical_end
            all_chunks.extend(chunks)
    
    return all_chunks



def create_typesense_index():
    schema = {
        "name": "transcripts",  
        "fields": [
            {"name": ".*", "type": "auto" },
            {"name": "id", "type": "auto", "index": False },
        ]
    }
    ts_client.collections.create(schema)

def upload_to_typesense(chunks):
    documents = []
    for chunk in chunks:
        doc_index = chunks.index(chunk)
        document = {
            'id': str(doc_index),
            'content': str(chunk)
        }
        documents.append(document)

        try:
            ts_client.collections['transcripts'].documents.upsert(document)
        except Exception as e:
            print(f"Failed at index {doc_index}")
            print(f"Failed document: {document}")
            raise e
    
        if doc_index % 100 == 0 and doc_index > 0:
            print(f"Uploaded {doc_index} documents of {len(chunks)}")
        
        # Should use the batch upload api but could not get it to work w/ JSON formatting

    print(f"Finished uploading {len(chunks)} documents")
   


chunks = create_chunks()
create_typesense_index()
upload_to_typesense(chunks)