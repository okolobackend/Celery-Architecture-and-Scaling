import time
from collections import defaultdict


def main():
    """
    Для значения 10^6 среднее время выполнения: 0.06 сек / 0.07 сек / 0.06 сек
    Для значения 10^7 среднее время выполнения: 0.62 сек / 0.68 сек / 0.63 сек
    Для значения 10^8 среднее время выполнения: 6.13 сек / 6.76 сек / 6.31 сек
    """
    aver_dict = defaultdict(list)

    for _ in range(10):
        for multiplier in [6, 7, 8]:
            start = time.time()

            result = 0
            for i in range(10 ** multiplier):
                result += i ** 2

            aver_dict[f"10^{multiplier}"].append(time.time() - start)

    # Просто красивые цвета на будущее
    # print("\033[91mКрасный текст\033[0m")
    # print("\033[92mЗелёный текст\033[0m")
    # print("\033[94mСиний текст\033[0m")
    colors_for_console = [("Красный текст", "\033[91m", "\033[0m"),
                          ("Зелёный текст", "\033[92m", "\033[0m"),
                          ("Синий текст", "\033[94m", "\033[0m")]

    for key, value in aver_dict.items():
        color, left_edge, right_edge = colors_for_console.pop(-1)
        print(f"{left_edge}Для значения {key} среднее время выполнения: {round(sum(value) / len(value), 2)} сек{right_edge}")


if __name__ == '__main__':
    main()
