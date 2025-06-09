%pip install -U llama-index llama-index-llms-databricks mlflow
%restart_python

from llama_index.llms.databricks import Databricks
from llama_index.core.llms import ChatMessage
from llama_index.core.agent.workflow import ReActAgent
from git import Repo
from github import Github
from databricks.sdk import WorkspaceClient
import asyncio

def suggest_fix(issue_summary):
    w = WorkspaceClient()
    tmp_token = w.tokens.create(comment="for model serving", lifetime_seconds=1200)
    llm = Databricks(
        model="databricks-llama-4-maverick",
        api_key=tmp_token,
        api_base=f"dbc-02e26b76-2786.cloud.databricks.com/serving-endpoints/databricks-llama-4-maverick/invocations"
    )
    prompt = f"""
                Here's the issue:
                {issue_summary}
                Suggest only a code fix to resolve it.
    """
    msgs = [
        ChatMessage(
            role="system", content="You are a senior data engineer tasked with fixing data pipeline problems with python, pyspark, and databricks. Your job is to suggest code fix recommendations for given pipeline failure problems."
        )
    ]
    llm.chat(msgs)

    resp = llm.complete(prompt)
    return resp


### AXED FOR TIME
# def edit_notebook(notebook_path):
#    pass


# grab most recently added fail reason row
fail_rsn_df = spark.sql("""
    select
        a.rsn_key,
        a.rsn,
        a.notebook_path
    from fail_rsn a
    inner join (
        select max(rsn_when) as max_rsn_when
        from fail_rsn
    ) b
    on a.rsn_when = b.max_rsn_when
""")
rsn_key = fail_rsn_df.collect()[0][0]
fail_rsn = fail_rsn_df.collect()[0][1]
notebook_path = fail_rsn_df.collect()[0][2]

# get failure fix suggestion from model
fail_fix_suggestion = asyncio.run(suggest_fix(fail_rsn))

spark.sql(f"""
insert into table fail_fix_suggestions (fail_rsn_key,fix_suggestion) values (
    {rsn_key},
    "{fail_fix_suggestion}"
)
""")



# ### THIS IS WHERE WE AXED THE REST FOR NOW FOR TIME REASONS

# # global repository in opt
# repo_path = "/opt/dais2025hackathon"
# notebook_path = f"{repo_path}/{notebook_path}"

# # Initialize the repo object
# repo = Repo(repo_path)

# # Fetch the latest changes from the remote repository
# origin = repo.remote(name="origin")
# origin.fetch()

# feature_branch_name = "fix-suggestion"
# # Create a new branch based on the remote 'main' branch
# new_branch = repo.create_head(feature_branch_name, "origin/main")

# # Checkout the newly created branch
# repo.git.checkout(feature_branch_name)

# #some function to edit the appropriate notebook
# edit_notebook(notebook_path)

# # add all changes
# repo.git.add(A=True)

# # commit changes with commit message
# commit_message = "Updated files with latest changes"
# repo.index.commit(commit_message)

# # Set up authentication (use a personal access token)
# token = "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"
# repo_name = "data-doc420/dais2025hackathon"
# base_branch = "main"  # Target branch

# # Initialize GitHub client
# g = Github(token)
# repo = g.get_repo(repo_name)

# # Create pull request
# pr = repo.create_pull(
#     title="Fix Suggestion",
#     body="This is a data doc fix suggestion for the requested pipeline failure.",
#     head=feature_branch_name,
#     base=base_branch
# )

# print("pull request submitted")