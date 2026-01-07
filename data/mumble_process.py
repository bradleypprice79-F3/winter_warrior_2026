import re
import pandas as pd

# Regex patterns
time_pattern = re.compile(r"\s+\d{1,2}:\d{2}\s[AP]M")

rows = []

current_pax = None
maybe_new_pax = None
current_time = None
current_post_lines = []

with open(r"data\mumble.txt","r", encoding="utf-8") as f:
    lines = [line.rstrip() for line in f]

def flush_post():
    """Save the current post if it exists."""
    if current_pax and current_time and current_post_lines:
        full_post = " ".join(current_post_lines[:-1]).strip()
        rows.append({
            "PAX": current_pax,
            "TIME": current_time,
            "POST_SNIPPET": full_post[:50]
        })

for line in lines:

    # Time line
    if time_pattern.match(line):
        # a new post has started, flush the current post.
        flush_post()
        # clear out the snippet
        current_post_lines = []
        # the previous line was the pax
        current_pax = maybe_new_pax
        #the current line is the time
        current_time = line

    # Post content
    else:
        if current_time:
            current_post_lines.append(line)

    maybe_new_pax = line


# Flush the final post, youll lose the last line but oh well.
flush_post()

# Create DataFrame
df = pd.DataFrame(rows)

# Count posts per PAX
pax_counts = df.groupby("PAX").size().reset_index(name="POST_COUNT")

# Sort from greatest to least
pax_counts = pax_counts.sort_values(by="POST_COUNT", ascending=False)

print(pax_counts)
total_posts = pax_counts.sum()
len(pax_counts)
print("Total posts:", total_posts)

pd.set_option("display.max_rows", None)
print(pax_counts)