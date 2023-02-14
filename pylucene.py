import logging, sys
logging.disable(sys.maxsize)

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

file_name = sys.argv[1]
query_word = sys.argv[2]

file_path="./posts/" + file_name + ".json"
f = open(file_path,'r+')
sample_doc = json.load(f)
global count 

def create_index(dir):
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

#Title, author, text, and all post comments are indexed.
    for sample in sample_doc:
        #count = count + 1
        title = sample['title']
        author = sample['author']
        text = sample['text']
        comments = sample["comments"]
        all_comments = []
        for comment in comments:
            all_comments.append(comment["text"])

        doc = Document()
        doc.add(Field('Title', str(title),metaType))
        doc.add(Field('Author', str(author), contextType))
        doc.add(Field('Text', str(text), contextType))
        doc.add(Field('Comments', str(all_comments), contextType))
        writer.addDocument(doc)

        #Condition to calculate the time required for processing in Lucene
        #if( count == 100 or  count == 200 or count == 300 or count == 400 or count == 500 or count == 600 or count == 700 or count == 800 or count == 900 ):
            print("Time taken at count ", count, " is ", time.time()  - start," seconds")
   
    writer.close()
   
    #end = time.time()
    
    #print("Time taken to create the index: ", end - start," seconds")

def retrieve(storedir, query):
    searchDir = NIOFSDirectory(Paths.get(storedir))
    searcher = IndexSearcher(DirectoryReader.open(searchDir))

    parser = MultiFieldQueryParser(['Title','Author','Text','Comments'],StandardAnalyzer())
    parser.setDefaultOperator(QueryParserBase.OR_OPERATOR)
    parsed_query = MultiFieldQueryParser.parse(parser,query)

    topDocs = searcher.search(parsed_query, 10).scoreDocs
    topkdocs = []
    for hit in topDocs:
        doc = searcher.doc(hit.doc)
        topkdocs.append({
            "score": hit.score,
            "Title": doc.get("Title"),
            "Text": doc.get("Text"),
            "Author": doc.get("Author"),
            "Comments": doc.get("Comments")
            })
        #To get the urls of the retrieved documents
    for i, docs in enumerate(topkdocs):
        topkdocs[i]["url"] = sample_doc[i]["url"]
        #Only relevant fields are printed
    for docs in topkdocs:
        print("Score: ", docs["score"])
        print("Title: ", docs["Title"])
        print("Url: ", docs["url"])
        print("Text: ", docs["Text"])
        print("Author: ", docs["Author"])
        #print("Comments: ", docs["Comments"])
        print("")

lucene.initVM(vmargs=['-Djava.awt.headless=true'])
create_index('sample_lucene_index/')
#Our sample query runs on query word and subreddit name passed as a argument 
retrieve('sample_lucene_index/', query_word)

