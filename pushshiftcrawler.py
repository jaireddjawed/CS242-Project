import os
import requests
import json
import time

def crawl_subreddit(subreddit):
    # Define API endpoint
    url = "https://api.pushshift.io/reddit/search/submission"

    # Define headers and parameters for API request
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
    params = {"subreddit": subreddit, "sort_type": "hot", "size": 100, "fields": "title,author,url,selftext,created_utc,upvote_ratio"}

    # Crawl 1000 hot posts
    print(f"Crawling posts from r/{subreddit}")
    posts = []
    before = None

    # Loop over batches of 100 posts
    for i in range(50):
        if before is not None:
            params["before"] = before

        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = json.loads(response.text)
            if len(data["data"]) == 0:
                break
            for post in data["data"]:
                if not post["over_18"]:
                    post_data = {}
                    for field in params["fields"].split(","):
                        try:
                            if field == "selftext":
                                post_data["text"] = post["selftext"]
                            else:
                                post_data[field] = post[field]
                        except KeyError:
                            post_data[field] = ""
                    posts.append(post_data)

            before = data["data"][-1]["created_utc"]
        else:
            print(f"Error {response.status_code}: {response.reason}")
            break

        # If we have fetched 1000 posts or less than 10 MB of data, write them to file
        if len(posts) == 1000 or response.content.__sizeof__() < 10000000 or i == 49:
            filename = f"./posts/{subreddit}_{i}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(posts, f, ensure_ascii=False, indent=4)
                print(f"Saved {len(posts)} hot posts from r/{subreddit} to {filename}")
            posts = []

        time.sleep(2)  # Respect rate limit of 1 request per second
    time.sleep(2)

def main():
    # Define subreddits to crawl
    config = json.load(open(os.path.join(os.getcwd(), 'config1.json')))
    subreddits = config['subreddits']

    # Crawl each subreddit
    for subreddit in subreddits:
        crawl_subreddit(subreddit)
        time.sleep(10)

if __name__ == "__main__":
    main()

