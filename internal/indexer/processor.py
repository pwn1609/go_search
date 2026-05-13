from urllib.parse import urlparse, urlunparse, unquote
from bs4 import BeautifulSoup

from elasticsearchclient import ESClient, Indexed_Page
from consumer import Consumer
from kafka import ConsumerRecord

class Processor:
    def __init__(self, elasticsearch: ESClient, kafka: Consumer) -> None:
        self.es_client = elasticsearch
        self.kfk_client = kafka

    def pull_messages(self) -> None:
        for msg in self.kfk_client:
            self.process_message(msg)

    def process_message(self, msg: ConsumerRecord) -> None:
        normalized_url = self.normalize_url(msg.key)
        page_title = self.get_title(msg.value)
        cleaned_text = self.process_html(msg.value)

        page = Indexed_Page(url=normalized_url, title=page_title, body=cleaned_text, timestamp=msg.timestamp)
        self.es_client.post_to_index(page)

    def normalize_url(self, url: str) -> str:
        if isinstance(url, bytes):
            url = url.decode("utf-8")
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        host = parsed.hostname.lower() if parsed.hostname else ""
        port = parsed.port
        path = unquote(parsed.path).rstrip("/") or "/"
        query = parsed.query

        # Only include port if non-standard
        if port and not (scheme == "http" and port == 80) and not (scheme == "https" and port == 443):
            netloc = f"{host}:{port}"
        else:
            netloc = host

        return urlunparse((scheme, netloc, path, "", query, ""))

    def get_title(self, html_body: str) -> str:
        if isinstance(html_body, bytes):
            html_body = html_body.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html_body, "html.parser")
        title_tag = soup.find("title")
        return title_tag.get_text(strip=True) if title_tag else ""

    def process_html(self, html_body: str) -> str:
        if isinstance(html_body, bytes):
            html_body = html_body.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html_body, "html.parser")

        # Remove non-visible elements
        for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
            tag.decompose()

        text = soup.get_text(separator=" ")
        # Collapse whitespace
        return " ".join(text.split())