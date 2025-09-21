import xlsxwriter

def main():
    data = {
        "1": "Нет ответа",
        "2": "Я продуктовый маркетолог в проб-техкомпании.",
        "3": "У нас сейчас суперактивная стадия исследования...",
        "4": "У нас сейчас два направления задач...",
        "5": "Нет ответа",
        "6": "Я продуктовый маркетолог в проб-техкомпании. Занимаемся сервисами...",
        "7": "Наша основная цель и работа — это количество сделок или прибыль продукта...",
        "8": "Что мы делаем? У нас есть огромное количество табличек...",
        "9": "Да, это прям проблема, потому что их прям очень много...",
        "10": "Нет ответа",
    }

    workbook = xlsxwriter.Workbook("answers_with_merge.xlsx")
    worksheet = workbook.add_worksheet("Ответы")

    # Форматы
    header_format = workbook.add_format({
        "bold": True, "bg_color": "#D9E1F2",
        "align": "center", "valign": "vcenter",
        "border": 1
    })
    cell_format = workbook.add_format({
        "text_wrap": True, "valign": "top", "border": 1
    })
    merged_format = workbook.add_format({
        "bold": True, "align": "center", "valign": "vcenter",
        "bg_color": "#FCE4D6", "border": 1
    })

    # Заголовки
    worksheet.write(0, 0, "№", header_format)
    worksheet.write(0, 1, "Ответ", header_format)
    worksheet.write(0, 2, "Комментарий", header_format)

    # Заполняем строки и объединяем ячейки в третьем столбце попарно
    row = 1
    for key, value in data.items():
        worksheet.write(row, 0, key, cell_format)
        worksheet.write(row, 1, value, cell_format)

        # Пример объединения: каждая пара строк в колонке C объединяется
        if row % 2 == 1:  # нечётная строка → начало пары
            worksheet.merge_range(row, 2, row + 1, 2, f"Комментарий для {key}-{int(key)+1}", merged_format)

        row += 1

    # Автоподбор ширины
    worksheet.set_column(0, 0, 5)
    worksheet.set_column(1, 1, 80)
    worksheet.set_column(2, 2, 40)

    workbook.close()
    print("Файл answers_with_merge.xlsx успешно создан!")

if __name__ == "__main__":
    main()
