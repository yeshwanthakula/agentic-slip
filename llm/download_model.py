from transformers import GPT2LMHeadModel, GPT2Tokenizer

def download_model():
    model_name = "distilgpt2"
    # Download to a specific location
    tokenizer = GPT2Tokenizer.from_pretrained(model_name, bos_token='<|startoftext|>', eos_token='<|endoftext|>', pad_token='<|pad|>')
    model = GPT2LMHeadModel.from_pretrained(model_name)
    
    # Save locally
    model.save_pretrained('app/model')
    tokenizer.save_pretrained('app/model')

if __name__ == "__main__":
    download_model()