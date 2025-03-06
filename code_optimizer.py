# code_optimizer.py
import google.generativeai as genai
import os
import git

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# Gemini API Configuration
genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))
model = genai.GenerativeModel('gemini-pro')

# Git Configuration (replace with your repo details)
REPO_PATH = "E:/edu-files/GitHub-Repo/"
FILE_PATH = "E:/edu-files/GitHub-Repo/cecl-synapse/"

def read_code(file_path):
    with open(file_path, "r") as f:
        return f.read()

def optimize_code(code):
    prompt = f"""
    Analyze the following PySpark code for potential performance bottlenecks and vulnerabilities:

    ```python
    {code}
    ```

    Identify:
    - Inefficient data transformations.
    - Potential data skew issues.
    - Opportunities for optimization.
    - Security vulnerabilities.
    - Suggest optimized code and explain the improvements.
    """
    response = model.generate_content(prompt)
    return response.text

def apply_code_changes(optimized_code):
    repo = git.Repo(REPO_PATH)
    with open(FILE_PATH, "w") as f:
        f.write(optimized_code)
    repo.index.add([FILE_PATH])
    repo.index.commit("Optimized PySpark code based on LLM analysis.")
    origin = repo.remote(name='origin')
    origin.push()

def main():
    code = read_code(FILE_PATH)
    analysis = optimize_code(code)
    print(analysis)
    user_input = input("Apply changes? (y/n): ")
    if user_input.lower() == "y":
        apply_code_changes(analysis)
        print("Changes applied and pushed to Git.")
    else:
        print("Changes not applied.")

if __name__ == "__main__":
    main()