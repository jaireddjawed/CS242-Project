import logging, sys
logging.disable(sys.maxsize)

from tqdm import tqdm
import time
import lucene
import os
import json
from org.apache.lucene.store import MMapDirectory, SimpleFSDirectory, NIOFSDirectory
from java.nio.file import Paths

#from org.apache.lucene.analysis import Analyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser, QueryParserBase
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader
from org.apache.lucene.search import IndexSearcher, BoostQuery, Query
from org.apache.lucene.search.similarities import BM25Similarity

dir_path = './posts/'
file_names = os.listdir(dir_path)

def create_index(dir='./sample_lucene_index/'):
    if os.path.exists(dir):
        return
    print("Creating Lucene Index!")
    count = 0
    #start = time.time()

    if not os.path.exists(dir):
        os.mkdir(dir)
    store = SimpleFSDirectory(Paths.get(dir))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(store, config)

    metaType = FieldType()
    metaType.setStored(True)
    metaType.setTokenized(False)

    contextType = FieldType()
    contextType.setStored(True)
    contextType.setTokenized(True)
    contextType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

    url_map = {}

    #Title, author, text, and all post comments are indexed.
    for file_name in file_names:
        with open(os.path.join(dir_path, file_name), 'r') as f:
            try:
                data = json.load(f)
            except json.decoder.JSONDecodeError as e:
                print(f"Error decoding JSON file {file_name}: {e}")
                continue  # skip to next file
        for sample in tqdm(data, desc=f'Processing {file_name}'):
            count = count + 1
            title = sample['title']
            author = sample['author']
            text = sample['text']
            """comments = sample["comments"]
            all_comments = []
            for comment in comments:
                all_comments.append(comment["text"])"""

            doc = Document()
            doc.add(Field('Title', str(title),metaType))
            doc.add(Field('Author', str(author), contextType))
            doc.add(Field('Text', str(text), contextType))
            #doc.add(Field('Comments', str(all_comments), contextType))
            writer.addDocument(doc)

            url_map[count-1] = sample['url']

        #Condition to calculate the time required for processing in Lucene
        #if( count == 100 or  count == 200 or count == 300 or count == 400 or count == 500 or count == 600 or count == 700 or count == 800 or count == 900 ):
            #print("Time taken at count ", count, " is ", time.time()  - start," seconds")
   
    writer.close()

    # Save url_map as JSON file
    with open('lucene.json', 'w') as f:
        json.dump(url_map, f)
   
    #end = time.time()
    
    #print("Time taken to create the index: ", end - start," seconds")


def retrieve(query, storedir='./sample_lucene_index/'):
    lucene.getVMEnv().attachCurrentThread()
    searchDir = NIOFSDirectory(Paths.get(storedir))
    searcher = IndexSearcher(DirectoryReader.open(searchDir))

    parser = MultiFieldQueryParser(['Title','Author','Text','Comments'], StandardAnalyzer())
    parser.setDefaultOperator(QueryParserBase.OR_OPERATOR)
    parsed_query = MultiFieldQueryParser.parse(parser, query)

    topDocs = searcher.search(parsed_query, 10).scoreDocs
    topkdocs = []
    with open('lucene.json') as f:
        url_map = json.load(f)
    for hit in topDocs:
        doc = searcher.doc(hit.doc)
        url = url_map[str(hit.doc)]
        topkdocs.append({
            "title": doc.get("Title"),
            "url": url
        })

    return topkdocs
lucene.initVM(vmargs=['-Djava.awt.headless=true'])
create_index()
#Our sample query runs on query word and subreddit name passed as a argument
#print(retrieve(sys.argv[1]))
