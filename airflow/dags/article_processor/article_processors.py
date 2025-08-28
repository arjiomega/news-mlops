from bs4 import BeautifulSoup
from article_processor.base import ArticleProcessor

class Google9to5ArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        article_content = article_soup.find("div", class_="post-content")
        if "ad-disclaimer-container" in str(article_content):
            article_content.find("div", class_="ad-disclaimer-container").decompose()
        texts = article_content.find_all("p")
        article_text = "\n\n".join(p.get_text(strip=True) for p in texts)

        return article_text

class AlJazeeraEnglishArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        texts = article_soup.find("main").find("div",class_="wysiwyg wysiwyg--all-content").find_all("p")
        article_text = "\n".join(p.get_text(strip=True) for p in texts)

        return article_text

class BBCNewsArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        texts = article_soup.find("article").find_all("p")
        article_text = "\n\n".join(p.get_text(strip=True) for p in texts)
        return article_text
    
class CNNArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        texts = article_soup.find("article").find_all("p")
        article_text = "\n\n".join(p.get_text(strip=True) for p in texts)
        return article_text

    def _get_title(self, article_soup: BeautifulSoup) -> str:
        return article_soup.find("h1").get_text(strip=True)
    
class GizmodoArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        texts = article_soup.find("div", class_="entry-content").find_all("p")
        article_text = "\n\n".join(p.get_text(strip=True) for p in texts)
        return article_text
    
class JalopnikArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        texts = article_soup.find("div", class_="article").find("article", class_="news-post").find_all("p")
        article_text = "\n\n".join(p.get_text(strip=True) for p in texts)
        return article_text
    
class MacRumorsArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        article_text = article_soup.find("article").find("div", class_="js-content").get_text(strip=True)
        return article_text

    def _get_title(self, article_soup: BeautifulSoup) -> str:
        return article_soup.find("h1").get_text(strip=True)
    
class NBCNewsArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        texts = article_soup.find("article").find_all("p")
        article_text = "\n\n".join(p.get_text(strip=True) for p in texts)
        return article_text

    def _get_title(self, article_soup: BeautifulSoup) -> str:
        return article_soup.find("h1").get_text(strip=True)
    
class NintendoLifeArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        texts = article_soup.find("article").find_all("p")
        article_text = "\n\n".join(p.get_text(strip=True) for p in texts)
        return article_text

    def _get_title(self, article_soup: BeautifulSoup) -> str:
        return article_soup.find("h1").get_text(strip=True)
    
class PhoronixArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        texts = article_soup.find("div", class_="content")
        article_text = texts.get_text(strip=True)
        return article_text
     
    def _get_title(self, article_soup: BeautifulSoup) -> str:
        return article_soup.find("h1").get_text(strip=True)
    
class PolygonArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        texts = article_soup.find("article").find_all("p")
        article_text = "\n\n".join(p.get_text(strip=True) for p in texts)
        return article_text
    
class TheVergeArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        texts = article_soup.find("article").find("div", class_="duet--layout--entry-body-container").find_all("p")
        
        article_texts = []
        for p in texts:
            if ("fv263x1" in p.get("class")) or ("fv263x4" in p.get("class")):
                continue
            article_texts.append(p.get_text(strip=True))

        article_text = "\n\n".join(article_texts)
        return article_text
    
class TomsGuideArticleProcessor(ArticleProcessor):
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        texts = article_soup.find("div", id="article-body").find_all("p")
        article_text = "\n\n".join(p.get_text(strip=True) for p in texts)
        return article_text

    def _get_title(self, article_soup: BeautifulSoup) -> str:
        return article_soup.find("h1").get_text(strip=True)