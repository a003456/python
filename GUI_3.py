
from tkinter import filedialog
import time
import pandas as pd
import chess.pgn
import warnings
import pygame
import chess
import chess.engine
import sys
import tkinter as tk
import os
import asyncio










warnings.filterwarnings("ignore")





directory_path = "F:\\Python\\CCC"
os.chdir(directory_path)





SQUARE_SIZE = 16
WIDTH, HEIGHT = SQUARE_SIZE * 8, SQUARE_SIZE * 8

WHITE, GRAY, GREEN, BLUE = (255, 255, 255), (125, 135, 150), (0, 255, 0), (70, 130, 180)


def load_pieces():
    pieces = {}
    for color in ['w', 'b']:
        for piece in ['p', 'r', 'n', 'b', 'q', 'k']:
            key = f"{color}{piece}"
            path = os.path.join("images", f"{key}.png")
            try:
                img = pygame.image.load(path)
                pieces[key] = pygame.transform.scale(img, (SQUARE_SIZE, SQUARE_SIZE))
            except pygame.error as e:
                print(f"Error loading image {path}: {e}")
    return pieces


def draw_board(screen, selected_square=None, legal_moves=[]):
    for row in range(8):
        for col in range(8):
            color = WHITE if (row + col) % 2 == 0 else GRAY
            rect = pygame.Rect(col*SQUARE_SIZE, row*SQUARE_SIZE, SQUARE_SIZE, SQUARE_SIZE)
            pygame.draw.rect(screen, color, rect)

    if selected_square is not None:
        col = chess.square_file(selected_square)
        row = 7 - chess.square_rank(selected_square)
        pygame.draw.rect(screen, BLUE, pygame.Rect(col*SQUARE_SIZE, row*SQUARE_SIZE, SQUARE_SIZE, SQUARE_SIZE), 4)

        for move in legal_moves:
            to_sq = move.to_square
            col = chess.square_file(to_sq)
            row = 7 - chess.square_rank(to_sq)
            pygame.draw.circle(screen, GREEN, (col * SQUARE_SIZE + SQUARE_SIZE//2, row * SQUARE_SIZE + SQUARE_SIZE//2), 10)

def draw_pieces(screen, board, piece_images, dragged_piece=None, dragged_pos=None):
    for square in chess.SQUARES:
        if dragged_piece and square == dragged_piece[0]:
            continue  # skip drawing dragged piece here

        piece = board.piece_at(square)
        if piece:
            row = 7 - chess.square_rank(square)
            col = chess.square_file(square)
            color = 'w' if piece.color == chess.WHITE else 'b'
            key = f"{color}{piece.symbol().lower()}"
            screen.blit(piece_images[key], (col * SQUARE_SIZE, row * SQUARE_SIZE))

    if dragged_piece:
        piece = dragged_piece[1]
        color = 'w' if piece.color == chess.WHITE else 'b'
        key = f"{color}{piece.symbol().lower()}"
        screen.blit(piece_images[key], (dragged_pos[0] - SQUARE_SIZE//2, dragged_pos[1] - SQUARE_SIZE//2))

def get_square_under_mouse(pos):
    x, y = pos
    col = x // SQUARE_SIZE
    row = 7 - (y // SQUARE_SIZE)
    if 0 <= col <= 7 and 0 <= row <= 7:
        return chess.square(col, row)
    return None



def get_legal_moves(board, square):
    return [move for move in board.legal_moves if move.from_square == square]

def play_engine_move(board, engine):
    if board.is_game_over():
        return
    result = engine.play(board, chess.engine.Limit(time=0.1))
    board.push(result.move)

def load_pgn_file():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(title="Select PGN File", filetypes=[("PGN Files", "*.pgn")])
    return file_path


def move_to_verbal(move):
    piece_names = {'K': 'King', 'Q': 'Queen', 'R': 'Rook', 'B': 'Bishop', 'N': 'Knight', 'p': 'Pawn'}

    # Identify check and mate
    is_check = '+' in move
    is_mate = '#' in move

    move = move.replace('+', '').replace('#', '')

    if move == 'O-O':
        result = "KING CASTLES KINGSIDE"
    elif move == 'O-O-O':
        result = "KING CASTLES QUEENSIDE"
    # Pawn move
    elif len(move) == 2 and move[0].isalpha() and move[1].isdigit():
        target = ' '.join(move)
        result = f"PAWN {target.upper()}"
    # Pawn promotion
    elif '=' in move:
        parts = move.split('=')
        target = ' '.join(parts[0])
        promotion_piece = piece_names.get(parts[1], parts[1])
        result = f"PAWN PROMOTES TO {promotion_piece.upper()} AT {target.upper()}"
    # Capture
    elif 'x' in move:
        piece = move[0] if move[0] in piece_names else 'p'
        target = ' '.join(move.split('x')[1])
        result = f"{piece_names[piece]} TAKES {target.upper()}"
    # Regular move
    else:
        piece = move[0] if move[0] in piece_names else 'p'
        target = ' '.join(move[1:])
        result = f"{piece_names[piece]} {target.upper()}"

    # Add check/mate notation
    if is_mate:
        result += "  MATE"
    elif is_check:
        result += "  CHECK"

    return result





def main():
    pgn_path = load_pgn_file()
    if not pgn_path:
        print("No PGN file selected.")
        return

    games = []
    with open(pgn_path) as pgn:
        while True:
            game = chess.pgn.read_game(pgn)
            if game is None:
                break
            games.append(game)

    if not games:
        print("No games found.")
        return

    pygame.init()
    screen = pygame.display.set_mode((WIDTH, HEIGHT))
    pygame.display.set_caption("Chess Repertoire Trainer")
    clock = pygame.time.Clock()
    font = pygame.font.SysFont(None, 30)
    piece_images = load_pieces()



    def replay_variation(node, move):
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                sys.exit()
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_DOWN or event.key == pygame.K_UP:
                    return event.key

        clock.tick(60)
        draw_board(screen)
        draw_pieces(screen, node.board(), piece_images)
        pygame.display.flip()

        pygame.time.delay(300)

        for variation in reversed(node.variations):
            result = replay_variation(variation, variation.move)
            if result in [pygame.K_DOWN, pygame.K_UP]:
                return result
        return None






    def train_white(node, move):
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                sys.exit()
            elif event.type == pygame.KEYDOWN:
                if event.key in [pygame.K_DOWN, pygame.K_UP]:
                    return event.key

        clock.tick(60)
        draw_board(screen)
        draw_pieces(screen, node.board(), piece_images)
        pygame.display.flip()

        pygame.time.delay(300)

        if node.board().turn == chess.WHITE:
            correct_move = node.variations[0].move if node.variations else None

            if not correct_move:
                # No moves to train on, just return
                return None

            waiting_for_correct_move = True

            selected_square = None
            legal_moves = []
            dragged_piece = None



            while waiting_for_correct_move:
                for event in pygame.event.get():
                    if event.type == pygame.QUIT:
                        pygame.quit()
                        sys.exit()

                    elif event.type == pygame.KEYDOWN:
                        if event.key in [pygame.K_DOWN, pygame.K_UP]:
                            return event.key

                        elif event.key == pygame.K_RIGHT:
                            waiting_for_correct_move = False

                    elif event.type == pygame.MOUSEBUTTONDOWN:
                        x, y = pygame.mouse.get_pos()
                        file = x // SQUARE_SIZE
                        rank = 7 - y // SQUARE_SIZE
                        selected_square = chess.square(file, rank)

                        if node.board().piece_at(selected_square) and node.board().color_at(selected_square) == chess.WHITE:
                            dragging = True
                            drag_piece = node.board().piece_at(selected_square)



                    elif event.type == pygame.MOUSEBUTTONUP and dragging:
                        x, y = pygame.mouse.get_pos()
                        file = x // SQUARE_SIZE
                        rank = 7 - y // SQUARE_SIZE
                        target_square = chess.square(file, rank)

                        move_candidate = chess.Move(selected_square, target_square)

                        matched_variation = None

                        if move_candidate == correct_move:
                            waiting_for_correct_move = False


                            if node.variations and node.board().turn == chess.BLACK:
                                waiting_for_correct_move = False



                        else:
                            print(f"Wrong move! Please play the {correct_move}.")

                draw_board(screen,selected_square,legal_moves)
                draw_pieces(screen, node.board(), piece_images)
                pygame.display.flip()
                clock.tick(60)

        for variation in reversed(node.variations):
            result = train_white(variation, variation.parent.move)
            if result in [pygame.K_DOWN, pygame.K_UP]:
                return result
        return None





    game_index = 0
    while True:
        if game_index < 0:
            game_index = 0
        elif game_index >= len(games):
            game_index = len(games) - 1

        game = games[game_index]

        # Choose mode
        # result = replay_variation(game, None)
        # result = train_white_1(game, None)
        result = train_white(game,None)

        if result == pygame.K_DOWN:
            game_index += 1
        elif result == pygame.K_UP:
            game_index -= 1
        else:
            game_index += 1
            if game_index >= len(games):
                print("All games completed.")
                break






if __name__ == "__main__":
    main()










