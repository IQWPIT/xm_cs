import json
import random
import time
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, simpledialog

COLS = 10
ROWS = 20
CELL = 30
PANEL_CELL = 20
BOARD_W = COLS * CELL
BOARD_H = ROWS * CELL
LEADERBOARD_FILE = Path(__file__).with_name("leaderboard.json")

BG = "#0B1020"
PANEL_BG = "#121933"
GRID_LINE = "#1B254A"
TEXT = "#EAF0FF"
SUBTEXT = "#9AA6CF"
GHOST = "#8A90AA"

SHAPES = {
    "I": [[1, 1, 1, 1]],
    "J": [[1, 0, 0], [1, 1, 1]],
    "L": [[0, 0, 1], [1, 1, 1]],
    "O": [[1, 1], [1, 1]],
    "S": [[0, 1, 1], [1, 1, 0]],
    "T": [[0, 1, 0], [1, 1, 1]],
    "Z": [[1, 1, 0], [0, 1, 1]],
}

COLORS = {
    "I": "#60DAFF",
    "J": "#5D7BFF",
    "L": "#FFAE57",
    "O": "#FFE177",
    "S": "#7FE08A",
    "T": "#D07BFF",
    "Z": "#FF6F86",
}

CLEAR_NAMES = {1: "Single", 2: "Double", 3: "Triple", 4: "Tetris"}
BASE_SCORES = {1: 100, 2: 300, 3: 500, 4: 800}


class TetrisApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("俄罗斯方块 - 优化版（流畅/玩法增强）")
        self.root.configure(bg=BG)

        self.frame = tk.Frame(root, bg=BG)
        self.frame.pack(padx=14, pady=14)

        self.board_canvas = tk.Canvas(self.frame, width=BOARD_W, height=BOARD_H, bg="#090D1A", highlightthickness=0)
        self.board_canvas.grid(row=0, column=0, rowspan=24, padx=(0, 14))

        self.sidebar = tk.Frame(self.frame, bg=PANEL_BG, padx=12, pady=12)
        self.sidebar.grid(row=0, column=1, sticky="n")

        self.score_var = tk.StringVar(value="0")
        self.level_var = tk.StringVar(value="1")
        self.lines_var = tk.StringVar(value="0")
        self.combo_var = tk.StringVar(value="0")
        self.message_var = tk.StringVar(value="按开始按钮开始")

        self._add_stat("分数", self.score_var, 0)
        self._add_stat("等级", self.level_var, 2)
        self._add_stat("消行", self.lines_var, 4)
        self._add_stat("连击", self.combo_var, 6)

        tk.Label(self.sidebar, text="下一个", fg=TEXT, bg=PANEL_BG, font=("Arial", 11, "bold")).grid(row=8, column=0, sticky="w", pady=(8, 4))
        self.next_canvas = tk.Canvas(self.sidebar, width=120, height=120, bg="#0B1020", highlightthickness=0)
        self.next_canvas.grid(row=9, column=0, sticky="w")

        tk.Label(self.sidebar, text="Hold", fg=TEXT, bg=PANEL_BG, font=("Arial", 11, "bold")).grid(row=10, column=0, sticky="w", pady=(10, 4))
        self.hold_canvas = tk.Canvas(self.sidebar, width=120, height=120, bg="#0B1020", highlightthickness=0)
        self.hold_canvas.grid(row=11, column=0, sticky="w")

        self.start_btn = tk.Button(self.sidebar, text="开始 / 重新开始", command=self.start_game, bg="#4F6CFF", fg="white", relief=tk.FLAT)
        self.start_btn.grid(row=12, column=0, sticky="ew", pady=(12, 5))

        self.pause_btn = tk.Button(self.sidebar, text="暂停", command=self.toggle_pause, bg="#38436D", fg="white", relief=tk.FLAT)
        self.pause_btn.grid(row=13, column=0, sticky="ew")

        tips = "操作：← → 移动，↑旋转，Z反转，↓软降，空格瞬降，C Hold"
        tk.Label(self.sidebar, text=tips, wraplength=220, justify="left", fg=SUBTEXT, bg=PANEL_BG).grid(row=14, column=0, sticky="w", pady=(10, 8))

        tk.Label(self.sidebar, textvariable=self.message_var, fg="#BFD0FF", bg=PANEL_BG, wraplength=220, justify="left").grid(row=15, column=0, sticky="w", pady=(0, 10))

        tk.Label(self.sidebar, text="排行榜（Top10）", fg=TEXT, bg=PANEL_BG, font=("Arial", 11, "bold")).grid(row=16, column=0, sticky="w")
        self.rank_list = tk.Listbox(self.sidebar, width=34, height=10, bg="#0B1020", fg=TEXT, highlightthickness=0, borderwidth=0)
        self.rank_list.grid(row=17, column=0, sticky="nsew", pady=(6, 0))

        self.frame.grid_columnconfigure(0, weight=1)
        self._bind_keys()

        self.loop_id = None
        self.last_tick = time.perf_counter()
        self.gravity_acc = 0.0
        self.lock_acc = 0.0

        self.reset_state()
        self.render_leaderboard()
        self.draw()

    def _add_stat(self, name, var, row):
        tk.Label(self.sidebar, text=name, fg=SUBTEXT, bg=PANEL_BG).grid(row=row, column=0, sticky="w")
        tk.Label(self.sidebar, textvariable=var, fg=TEXT, bg=PANEL_BG, font=("Arial", 13, "bold")).grid(row=row + 1, column=0, sticky="w")

    def _bind_keys(self):
        self.root.bind("<Left>", lambda _: self.move(-1))
        self.root.bind("<Right>", lambda _: self.move(1))
        self.root.bind("<Down>", lambda _: self.soft_drop())
        self.root.bind("<Up>", lambda _: self.rotate_current(clockwise=True))
        self.root.bind("z", lambda _: self.rotate_current(clockwise=False))
        self.root.bind("Z", lambda _: self.rotate_current(clockwise=False))
        self.root.bind("<space>", lambda _: self.hard_drop())
        self.root.bind("c", lambda _: self.hold_piece())
        self.root.bind("C", lambda _: self.hold_piece())

    def reset_state(self):
        self.board = [[0 for _ in range(COLS)] for _ in range(ROWS)]
        self.current = None
        self.next_piece = None
        self.hold = None
        self.can_hold = True
        self.bag = []

        self.score = 0
        self.lines = 0
        self.level = 1
        self.combo = -1
        self.back_to_back = False

        self.game_over = True
        self.paused = False
        self.gravity_acc = 0.0
        self.lock_acc = 0.0

    def get_gravity_interval(self):
        return max(0.08, 0.75 - (self.level - 1) * 0.06)

    def refill_bag(self):
        self.bag = list(SHAPES.keys())
        random.shuffle(self.bag)

    def random_piece(self):
        if not self.bag:
            self.refill_bag()
        p_type = self.bag.pop()
        shape = [row[:] for row in SHAPES[p_type]]
        return {"type": p_type, "shape": shape, "x": (COLS - len(shape[0])) // 2, "y": -1}

    @staticmethod
    def rotate(shape, clockwise=True):
        if clockwise:
            return [[shape[r][c] for r in range(len(shape) - 1, -1, -1)] for c in range(len(shape[0]))]
        return [[shape[r][c] for r in range(len(shape))] for c in range(len(shape[0]) - 1, -1, -1)]

    def has_collision(self, piece, dx=0, dy=0, test_shape=None):
        shape = test_shape or piece["shape"]
        for y, row in enumerate(shape):
            for x, cell in enumerate(row):
                if not cell:
                    continue
                nx = piece["x"] + x + dx
                ny = piece["y"] + y + dy
                if nx < 0 or nx >= COLS or ny >= ROWS:
                    return True
                if ny >= 0 and self.board[ny][nx]:
                    return True
        return False

    def spawn_piece(self):
        self.current = self.next_piece or self.random_piece()
        self.next_piece = self.random_piece()
        self.can_hold = True
        self.lock_acc = 0.0
        if self.has_collision(self.current):
            self.finish_game()

    def merge_piece(self):
        for y, row in enumerate(self.current["shape"]):
            for x, cell in enumerate(row):
                if cell:
                    by = self.current["y"] + y
                    bx = self.current["x"] + x
                    if by >= 0:
                        self.board[by][bx] = self.current["type"]

    def clear_lines(self):
        full = [idx for idx, row in enumerate(self.board) if all(row)]
        if not full:
            self.combo = -1
            self.message_var.set("继续堆叠，争取高连击！")
            return

        for idx in reversed(full):
            del self.board[idx]
        for _ in full:
            self.board.insert(0, [0] * COLS)

        cleared = len(full)
        self.lines += cleared
        self.level = self.lines // 10 + 1
        self.combo += 1

        base = BASE_SCORES[cleared] * self.level
        b2b_bonus = 0
        if cleared == 4:
            if self.back_to_back:
                b2b_bonus = int(base * 0.5)
            self.back_to_back = True
        else:
            self.back_to_back = False

        combo_bonus = max(0, self.combo) * 50 * self.level
        self.score += base + b2b_bonus + combo_bonus

        clear_name = CLEAR_NAMES.get(cleared, "Clear")
        text = f"{clear_name}! +{base}"
        if b2b_bonus:
            text += f" / B2B +{b2b_bonus}"
        if combo_bonus:
            text += f" / Combo +{combo_bonus}"
        self.message_var.set(text)

    def hold_piece(self):
        if self.game_over or self.paused or not self.current or not self.can_hold:
            return

        current_type = self.current["type"]
        if self.hold is None:
            self.hold = current_type
            self.spawn_piece()
        else:
            swap = self.hold
            self.hold = current_type
            shape = [row[:] for row in SHAPES[swap]]
            self.current = {"type": swap, "shape": shape, "x": (COLS - len(shape[0])) // 2, "y": -1}
            if self.has_collision(self.current):
                self.finish_game()

        self.can_hold = False
        self.draw()

    def move(self, direction):
        if self.game_over or self.paused or not self.current:
            return
        if not self.has_collision(self.current, dx=direction):
            self.current["x"] += direction
            self.lock_acc = 0.0
            self.draw()

    def soft_drop(self):
        if self.game_over or self.paused or not self.current:
            return
        if not self.has_collision(self.current, dy=1):
            self.current["y"] += 1
            self.score += 1
            self.draw()

    def hard_drop(self):
        if self.game_over or self.paused or not self.current:
            return
        dist = 0
        while not self.has_collision(self.current, dy=1):
            self.current["y"] += 1
            dist += 1
        self.score += dist * 2
        self.lock_piece_and_spawn()
        self.draw()

    def rotate_current(self, clockwise=True):
        if self.game_over or self.paused or not self.current:
            return
        rotated = self.rotate(self.current["shape"], clockwise=clockwise)
        kicks = [(0, 0), (-1, 0), (1, 0), (-2, 0), (2, 0), (0, -1)]
        for dx, dy in kicks:
            if not self.has_collision(self.current, dx=dx, dy=dy, test_shape=rotated):
                self.current["shape"] = rotated
                self.current["x"] += dx
                self.current["y"] += dy
                self.lock_acc = 0.0
                self.draw()
                return

    def lock_piece_and_spawn(self):
        self.merge_piece()
        self.clear_lines()
        self.spawn_piece()

    def get_ghost_y(self):
        if not self.current:
            return 0
        y = self.current["y"]
        while not self.has_collision(self.current, dy=(y - self.current["y"] + 1)):
            y += 1
        return y

    def draw_cell(self, canvas, x, y, color, size=CELL, outline=GRID_LINE):
        x1, y1 = x * size, y * size
        canvas.create_rectangle(x1, y1, x1 + size, y1 + size, fill=color, outline=outline)

    def draw_grid(self):
        for x in range(COLS + 1):
            px = x * CELL
            self.board_canvas.create_line(px, 0, px, BOARD_H, fill="#121A36")
        for y in range(ROWS + 1):
            py = y * CELL
            self.board_canvas.create_line(0, py, BOARD_W, py, fill="#121A36")

    def draw_mini_piece(self, canvas, p_type):
        canvas.delete("all")
        if not p_type:
            return
        shape = SHAPES[p_type]
        ox = (120 - len(shape[0]) * PANEL_CELL) // 2
        oy = (120 - len(shape) * PANEL_CELL) // 2
        for y, row in enumerate(shape):
            for x, cell in enumerate(row):
                if cell:
                    x1, y1 = ox + x * PANEL_CELL, oy + y * PANEL_CELL
                    canvas.create_rectangle(x1, y1, x1 + PANEL_CELL, y1 + PANEL_CELL, fill=COLORS[p_type], outline=GRID_LINE)

    def draw(self):
        self.board_canvas.delete("all")
        self.draw_grid()

        for y, row in enumerate(self.board):
            for x, cell in enumerate(row):
                if cell:
                    self.draw_cell(self.board_canvas, x, y, COLORS[cell])

        if self.current:
            ghost_y = self.get_ghost_y()
            for y, row in enumerate(self.current["shape"]):
                for x, cell in enumerate(row):
                    if not cell:
                        continue
                    gy = ghost_y + y
                    if gy >= 0:
                        self.draw_cell(self.board_canvas, self.current["x"] + x, gy, GHOST, outline="#616882")

            for y, row in enumerate(self.current["shape"]):
                for x, cell in enumerate(row):
                    if cell and self.current["y"] + y >= 0:
                        self.draw_cell(self.board_canvas, self.current["x"] + x, self.current["y"] + y, COLORS[self.current["type"]])

        if self.paused and not self.game_over:
            self.board_canvas.create_rectangle(0, 0, BOARD_W, BOARD_H, fill="#000000", stipple="gray50", outline="")
            self.board_canvas.create_text(BOARD_W // 2, BOARD_H // 2, text="PAUSED", fill=TEXT, font=("Arial", 30, "bold"))

        self.draw_mini_piece(self.next_canvas, self.next_piece["type"] if self.next_piece else None)
        self.draw_mini_piece(self.hold_canvas, self.hold)

        self.score_var.set(str(self.score))
        self.level_var.set(str(self.level))
        self.lines_var.set(str(self.lines))
        self.combo_var.set(str(max(0, self.combo)))

    def toggle_pause(self):
        if self.game_over:
            return
        self.paused = not self.paused
        self.pause_btn.configure(text="继续" if self.paused else "暂停")
        self.message_var.set("已暂停" if self.paused else "继续游戏")
        self.draw()

    def tick(self):
        now = time.perf_counter()
        dt = now - self.last_tick
        self.last_tick = now

        if not self.game_over and not self.paused and self.current:
            self.gravity_acc += dt
            gravity_interval = self.get_gravity_interval()

            while self.gravity_acc >= gravity_interval:
                self.gravity_acc -= gravity_interval
                if not self.has_collision(self.current, dy=1):
                    self.current["y"] += 1
                else:
                    break

            if self.has_collision(self.current, dy=1):
                self.lock_acc += dt
                if self.lock_acc >= 0.45:
                    self.lock_piece_and_spawn()
                    self.lock_acc = 0.0
            else:
                self.lock_acc = 0.0

            self.draw()

        self.loop_id = self.root.after(16, self.tick)

    def start_game(self):
        self.reset_state()
        self.game_over = False
        self.last_tick = time.perf_counter()
        self.next_piece = self.random_piece()
        self.spawn_piece()
        self.pause_btn.configure(text="暂停")
        self.message_var.set("游戏开始！尝试连击和 Tetris！")
        self.draw()

    def finish_game(self):
        self.game_over = True
        self.message_var.set("游戏结束")
        self.draw()
        messagebox.showinfo("游戏结束", f"你的得分是 {self.score}")
        self.try_save_score()

    def load_leaderboard(self):
        if not LEADERBOARD_FILE.exists():
            return []
        try:
            data = json.loads(LEADERBOARD_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            return []
        return []

    def save_leaderboard(self, entries):
        LEADERBOARD_FILE.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")

    def try_save_score(self):
        if self.score <= 0:
            return
        name = simpledialog.askstring("保存成绩", "输入昵称（最多12字符）：", parent=self.root)
        if not name:
            return
        name = name.strip()[:12]
        if not name:
            return

        entries = self.load_leaderboard()
        entries.append({"name": name, "score": self.score, "level": self.level, "lines": self.lines})
        entries.sort(key=lambda x: (x.get("score", 0), x.get("level", 0), x.get("lines", 0)), reverse=True)
        self.save_leaderboard(entries[:10])
        self.render_leaderboard()

    def render_leaderboard(self):
        entries = self.load_leaderboard()
        self.rank_list.delete(0, tk.END)
        if not entries:
            self.rank_list.insert(tk.END, "暂无记录")
            return

        for idx, item in enumerate(entries, start=1):
            line = f"{idx:>2}. {item.get('name', '玩家'):<10} {item.get('score', 0):>6}分 Lv.{item.get('level', 1)}"
            self.rank_list.insert(tk.END, line)


def main():
    root = tk.Tk()
    app = TetrisApp(root)
    app.tick()
    root.mainloop()


if __name__ == "__main__":
    main()
