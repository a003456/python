import time

import pandas as pd
import chess.pgn
import warnings
import pygame
import chess
import chess.engine
import os
import sys
import tkinter as tk
from tkinter import filedialog
import os
from playsound import playsound
import asyncio
from edge_tts import Communicate










warnings.filterwarnings("ignore")




pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', '{:.2f}'.format)




directory_path = "D:\\algo_trading\\lichess"
os.chdir(directory_path)




STOCKFISH_PATH = "stk_fish/stockfish/stockfish-windows-x86-64-avx2.exe"






WIDTH, HEIGHT = 640, 640
SQUARE_SIZE = WIDTH // 8
WHITE, GRAY, GREEN, BLUE = (255, 255, 255), (125, 135, 150), (0, 255, 0), (70, 130, 180)


def load_pieces():
    pieces = {}
    for color in ['w', 'b']:
        for piece in ['p', 'r', 'n', 'b', 'q', 'k']:
            key = f"{color}{piece}"
            path = os.path.join("images", f"{key}.png")
            img = pygame.image.load(path)
            pieces[key] = pygame.transform.scale(img, (SQUARE_SIZE, SQUARE_SIZE))
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

def display_status(screen, board, font):
    if board.is_checkmate():
        text = "Checkmate!"
    elif board.is_stalemate():
        text = "Stalemate!"
    elif board.is_check():
        text = "Check!"
    else:
        text = f"{'White' if board.turn else 'Black'} to move"

    surface = font.render(text, True, (0, 0, 0))
    screen.blit(surface, (10, 10))

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

    # Remove check and mate symbols from the move for processing
    move = move.replace('+', '').replace('#', '')

    # Special moves: Castling
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





def generate_audio(comment, output_file):
    comment = comment.replace("\n", " ")
    try:
        if os.path.exists(output_file) and os.path.getsize(output_file) > 2048:
            return
        async def _generate():
            communicator = Communicate(text=comment, voice="en-IN-PrabhatNeural", rate="+50%")
            await communicator.save(output_file)
        asyncio.run(_generate())
    except Exception as e:
        print(f"Error generating audio: {e}")











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

    game = games[0]
    board = game.board()

    selected_square = None
    dragged_piece = None
    legal_moves = []

    def replay_variation(node, move):
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                sys.exit()

        clock.tick(60)
        draw_board(screen)
        draw_pieces(screen, node.board(), piece_images)
        display_status(screen, node.board(), font)
        pygame.display.flip()

        if move:
            try:
                verbal_move = node.parent.board().san(move)
                verbal_move = move_to_verbal(verbal_move)
            except:
                verbal_move = move.uci()
        else:
            verbal_move = ""

        comment = f"{verbal_move} {node.comment}" if node.comment else verbal_move
        if comment.strip():
            path = os.path.join("audio", f"{comment}.mp3")
            generate_audio(comment, path)
            playsound(path)

        pygame.time.delay(300)

        for variation in reversed(node.variations):
            replay_variation(variation, variation.move)




    def train_white(node):



        if not node.variations:
            return

        for variation in node.variations:
            expected_node = variation
            expected_move = expected_node.move
            board = node.board().copy()

            move_done = False
            selected_square = None
            dragged_piece = None
            legal_moves = []
            while not move_done:


                draw_board(screen, selected_square, legal_moves)
                draw_pieces(screen, board, piece_images, dragged_piece, pygame.mouse.get_pos() if dragged_piece else None)
                display_status(screen, board, font)
                pygame.display.flip()

                for event in pygame.event.get():
                    if event.type == pygame.QUIT:
                        pygame.quit()
                        sys.exit()

                    elif event.type == pygame.MOUSEBUTTONDOWN:
                        square = get_square_under_mouse(pygame.mouse.get_pos())
                        if square is not None:
                            piece = board.piece_at(square)
                            if piece and piece.color == chess.WHITE:
                                selected_square = square
                                legal_moves = [m for m in board.legal_moves if m.from_square == square]
                                dragged_piece = (square, piece)

                    elif event.type == pygame.MOUSEBUTTONUP:
                        if dragged_piece:
                            from_sq = dragged_piece[0]
                            to_sq = get_square_under_mouse(pygame.mouse.get_pos())
                            move = chess.Move(from_sq, to_sq)

                            if move == expected_move:
                                board.push(move)
                                selected_square = None
                                dragged_piece = None
                                legal_moves = []
                                move_done = True
                            else:
                                verbal_move = move_to_verbal(expected_node.parent.board().san(expected_move))
                                error_msg = f"Incorrect move. Expected {verbal_move}"
                                path = os.path.join("audio", f"{error_msg}.mp3")
                                generate_audio(error_msg, path)
                                playsound(path)

                                board = node.board().copy()
                                selected_square = None
                                dragged_piece = None
                                legal_moves = []

            # After correct move, recurse into that variation
            train_white(expected_node)



    # replay_variation(game, None)
    # replay_variation_2(game, None)
    board = game.board()  # Reset board before training
    train_white(game)

if __name__ == "__main__":
    main()










