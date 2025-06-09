import openai
openai.api_key = dbutils.secrets.get("openai", "api_key")
def suggest_fix(issue_summary):
   prompt = f"""
   You're a senior data engineer. Here's the issue:
   {issue_summary}
   Suggest a code fix or explanation to resolve it.
   """
   response = openai.ChatCompletion.create(
       model="gpt-4",
       messages=[{"role": "user", "content": prompt}]
   )
   return response["choices"][0]["message"]["content"]
# Example use
issue = "Pipeline failed due to missing column 'order_status'."
print(suggest_fix(issue))