import difflib
import numpy as np 

from typing import List, Optional
from sentence_transformers import SentenceTransformer
from sentence_transformers import util

import operator as op
import os 

class KNNSearch:
    def __init__(self, model_name:str):
        self.model_name = model_name 
    
    def build_index(self, corpus:List[str]):
        self.corpus_embeddings = self.model.encode(corpus, convert_to_tensor=True, show_progress_bar=True)
        self.corpus = corpus 

    def search(self, query:str, top_k:int) -> List[str]:
        query_embedding = self.model.encode(query, convert_to_tensor=True)
        scores = query_embedding @ self.corpus_embeddings.T
        selected_candidates = sorted(list(zip(scores, self.corpus)), key=op.itemgetter(0), reverse=True)[:top_k]
        return list(map(op.itemgetter(1), selected_candidates))

    def __enter__(self):
        self.model = SentenceTransformer(self.model_name, cache_folder=os.getenv('TRANSFORMERS_CACHE'))
        print(self.model)
        return self 

    def __exit__(self, exc_type, exc_value, traceback):
        pass 


if __name__ == '__main__':
    with KNNSearch(model_name='distiluse-base-multilingual-cased-v1') as knn_search:
        knn_search.build_index(corpus=[
            "jëzbu", "wakaana hàqqan", "walaxadd karamnaa", "mawaahibu naafih", "mawahibul xuduus", "wa xul jaahal hàqq", "yasuuru", "sindiidi", "tuhfatu"
        ])
        keep_loop = True 
        while keep_loop:
            try:
                query = input("Enter query: ")
                hits = knn_search.search(query, top_k=5)
                print(hits)
                scores = [ (title, difflib.SequenceMatcher(None, query, title).find_longest_match(0, len(query), 0, len(title)).size) for title in hits]
                response = sorted(scores, key=op.itemgetter(1), reverse=True)
                print(response)
                titles = list(map(op.itemgetter(0), response))
                scores = list(map(op.itemgetter(1), response))

                scores = np.array(scores)
                max_  = np.max(scores)
                min_ = np.min(scores)
                scores = (scores - min_) / (max_ - min_)

                rsp = list(zip(titles, scores))
                print([a[0] for a in rsp if a[1] > 0.6])

            except KeyboardInterrupt:
                keep_loop = False
            except Exception as e:
                print(f"Error: {e}")