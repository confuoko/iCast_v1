from core.json_templates import INTERVIEW_TEMPLATE


def build_prompt():
    lines = []
    counter = 1
    for block in INTERVIEW_TEMPLATE:
        type_name = block.get("type")
        for subtype in block.get("subtypes", []):
            subtype_name = subtype.get("subtype")
            for q in subtype.get("questions", []):
                question = q.get("text")
                lines.append(f"{counter}. Вопрос типа {type_name} c целью выяснить {subtype_name}: {question}")
                counter += 1
    prompt_text = "\n".join(lines)
    print(prompt_text)
    return prompt_text


if __name__ == "__main__":
    build_prompt()
