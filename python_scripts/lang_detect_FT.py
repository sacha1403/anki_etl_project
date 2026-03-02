import re
import fasttext
from langdetect import detect, LangDetectException
import time

MODEL = fasttext.load_model("python_scripts/lid.176.bin")

def remove_html_tags(text):
        """Removes HTML tags from the input text."""
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)

def clean_text(text):
    """
    Cleans the input text by removing symbols (except hyphens within words).
    Removes text within [], (), and <...> tags from a string."""
    text = text.lower()
    text = text.replace("\n"," ")
    text = text.replace('&nbsp;', ' ') # Replace newlines with spaces
    text = re.sub(r'\[.*?\]', '', text)  # Remove text within square brackets
    text = re.sub(r'\(.*?\)', '', text)  # Remove text within parentheses
    text = re.sub(r'<.*?>', '', text)    # Remove HTML/XML tags
    regex = r"(?<!\w)[^\w\s-]|\d|[^\w\s-]+(?!\w)" # The expression that do the magic.
    cleaned_text = re.sub(regex, '', text)  # Replace matching characters with an empty string
    return cleaned_text

def detect_language_for_note(fields, fallback = False):
    """Detect the language of the first field in a list of fields, handling errors."""
    cleaned_field_cache = []
    if not isinstance(fields, list):
        return None, None

    languages = ['pt', 'pl', 'it']
    try_count = 0
    while try_count < min(3, len(fields)):
        if try_count == min(3, len(fields)) - 1 and not fallback: # If we are at the last field and we haven't done the fallback yet, we will do it
            fallback = True
            try_count = 0

        if not fallback:
            field = fields[try_count]
            if not isinstance(field, str):
                logging.warning(f"Field is not a string {field}. Skipping.")
                try_count += 1
                continue
            cleaned_field = clean_text(field)
            cleaned_field_cache.append(cleaned_field)

        else:
            if try_count >= len(cleaned_field_cache):
                try_count += 1
                continue
            cleaned_field = cleaned_field_cache[try_count]
        
        nb_words = len(cleaned_field.split()) 
        if len(cleaned_field.strip()) <= 1 or nb_words > 3 or nb_words == 0: # Skip language detection for the row if there is a field with more than 3 words or empty fields
        
            try_count += 1
            continue

        if not fallback:
            language = detect_lang_from_fasttext(cleaned_field)
        else:
            language = detect_lang_from_langdetect(cleaned_field)

        if (language in languages):
            return language, cleaned_field

        try_count += 1
    return None, None 


def detect_lang_from_fasttext(word):
    prediction = MODEL.predict(word)
    #lang_code = prediction[0][0].replace("__label__", "")
    #confidence = prediction[1][0]
    return prediction[0][0].replace("__label__", "")   

def detect_lang_from_langdetect(field):
    try:
        return detect(field)
    except LangDetectException as e:
        logging.warning(f"Language detection failed for field: '{field}'. Returning None. Error: {e}")
        return None, None

