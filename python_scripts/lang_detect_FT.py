import re
import fasttext
import time

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

def detect_language(fields):
    """Detect the language of the first field in a list of fields, handling errors."""
    t0 = time.time()
    if not isinstance(fields, list):
        return None, None, time.time() - t0

    languages = ['pt', 'pl', 'it']
    for i in range(0, min(3, len(fields))):
        field = fields[i]
        if not isinstance(field, str):
            logging.warning(f"Field is not a string {field}. Skipping.")
            continue

        cleaned_field = clean_text(field)
        nb_words = len(cleaned_field.split()) 
        if nb_words > 3 or nb_words == 0: # Skip language detection for the row if there is field with more than 3 words or empty fields
            return None, None, time.time() - t0

        if len(cleaned_field.strip()) <= 1:
            continue

        language = detect(cleaned_field)
           
        if (language in languages):
            return language, cleaned_field, time.time() - t0
    return None, None, time.time() - t0  


def detect(word):
    model = fasttext.load_model("python_scripts/lid.176.bin")
    prediction = model.predict(word)
    #lang_code = prediction[0][0].replace("__label__", "")
    #confidence = prediction[1][0]
    return prediction[0][0].replace("__label__", "")

