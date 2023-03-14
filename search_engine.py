from transformers import AutoTokenizer, AutoModel
from sklearn.metrics.pairwise import cosine_similarity

import logging, sys
logging.disable(sys.maxsize)

import time
import torch
import json
import os
import faiss
import numpy as np
from transformers import AutoTokenizer, AutoModel
from tqdm import tqdm

# Set Faiss index parameters
index_dim = 768
nlist = 100
quantizer = faiss.IndexFlatIP(index_dim)
index = faiss.IndexIVFFlat(quantizer, index_dim, nlist, faiss.METRIC_INNER_PRODUCT)
index.nprobe = 10  # number of probes at query time

# Set directory path and file names
dir_path = './posts/'
dir_path_map = './'

max_seq_length = 512  # maximum input length for BERT
stride = 128          # overlap between segments

# Load BERT tokenizer and model
tokenizer = AutoTokenizer.from_pretrained('distilbert-base-uncased')
model = AutoModel.from_pretrained('distilbert-base-uncased')

def create_index_and_map(dir_path, max_seq_length, stride, index_dim=768, nlist=100, nprobe=10):

    if os.path.exists(dir_path_map + 'map.json'):
        return

    print("Creating BERT Index!")
    # Set Faiss index parameters
    quantizer = faiss.IndexFlatIP(index_dim)
    index = faiss.IndexIVFFlat(quantizer, index_dim, nlist, faiss.METRIC_INNER_PRODUCT)
    index.nprobe = nprobe  # number of probes at query time

    # Set directory path and file names
    file_names = os.listdir(dir_path)

    doc_embeddings = []
    url_map = {}

    # Loop through each file and generate embeddings
    for file_name in file_names:
        with open(os.path.join(dir_path, file_name), 'r') as f:
            try:
                data = json.load(f)
            except json.decoder.JSONDecodeError as e:
                print(f"Error decoding JSON file {file_name}: {e}")
                continue  # skip to next file


        for doc in tqdm(data, desc=f'Processing {file_name}'):
            # Combine title, text, and comment text
            input_text = doc['title'] + ' ' + doc['text']
            # Split input text into segments with overlapping stride
            input_segments = []
            start = 0
            while start < len(input_text):
                end = min(len(input_text), start + max_seq_length)
                input_segments.append(input_text[start:end])
                start += max_seq_length - stride

            # Generate embeddings for each input segment and take their mean
            segment_embeddings = []
            for segment in input_segments:
                # Tokenize and truncate input segment
                inputs = tokenizer(segment, truncation=True, max_length=max_seq_length, padding='max_length')
                input_ids = np.array(inputs['input_ids']).reshape(1, -1).astype('int64')
                attention_mask = np.array(inputs['attention_mask']).reshape(1, -1).astype('float32')

                # Generate segment embedding
                with torch.no_grad():
                    input_ids = torch.from_numpy(input_ids)
                    attention_mask = torch.from_numpy(attention_mask)
                    outputs = model(input_ids, attention_mask=attention_mask)
                    embedding = outputs.last_hidden_state.mean(dim=1).squeeze().cpu().numpy()
                    segment_embeddings.append(embedding)

            if len(segment_embeddings) > 0:
                # Take the mean of all segment embeddings to get document embedding
                doc_embedding = np.mean(np.array(segment_embeddings), axis=0)
                doc_embeddings.append(doc_embedding)

                # Store url for document
                url_map[len(doc_embeddings) - 1] = {'url': doc['url'], 'title': doc['title']}

    # Convert embeddings to Faiss index format
    doc_embeddings = np.array(doc_embeddings).astype('float32')
    index.train(doc_embeddings)
    index.add(doc_embeddings)

    # Save index and url map
    faiss.write_index(index, os.path.join(dir_path_map, 'index'))
    with open(os.path.join(dir_path_map, 'map.json'), 'w') as f:
        json.dump(url_map, f)
    

# Define function to retrieve top k most relevant documents for a given query
def retrieve_documents(query, k=10):

    # Load index and url map
    index = faiss.read_index('./index')
    with open('./map.json', 'r') as f:
        url_map = json.load(f)
    # Tokenize and truncate query
    inputs = tokenizer(query, truncation=True, max_length=512, padding='max_length')
    input_ids = np.array(inputs['input_ids']).reshape(1, -1).astype('int64')
    attention_mask = np.array(inputs['attention_mask']).reshape(1, -1).astype('float32')

    # Generate query embedding
    with torch.no_grad():
        input_ids = torch.from_numpy(input_ids)
        attention_mask = torch.from_numpy(attention_mask)
        outputs = model(input_ids, attention_mask=attention_mask)
        query_embedding = outputs.last_hidden_state.mean(dim=1).squeeze().cpu().numpy()

    # Search index for top k most relevant documents
    distances, indices = index.search(np.array([query_embedding]).astype('float32'), k)

    # Retrieve url and title text for top k documents
    results = []
    for i in range(k):
        idx = indices[0][i]
        distance = distances[0][i]
        url = url_map[str(idx)]['url']
        title = url_map[str(idx)]['title']
        results.append({'url': url, 'title': title})

    return results

create_index_and_map(dir_path, max_seq_length, stride)
