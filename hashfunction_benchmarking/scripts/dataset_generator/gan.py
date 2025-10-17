# ========== SETUP ==========
# !pip install -r requirements.txt

from __future__ import annotations

import sys
import threading
from loguru import logger

import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader

import argparse
import textwrap
import pyfiglet
import pandas as pd
from loguru import logger
from rich.progress import Progress, TaskID, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn

class Generator(nn.Module):
    def __init__(self, latent_dim, output_len, vocab_size):
        super().__init__()
        self.model = nn.Sequential(
            nn.Linear(latent_dim, 128),
            nn.ReLU(),
            nn.Linear(128, output_len * vocab_size)
        )
        self.output_len = output_len
        self.vocab_size = vocab_size

    @staticmethod
    def load(latent_dim: int, max_len: int, vocab: set, device, path: str) -> Generator:
        logger.info(f"Loading Generator model={path}")
        generator: Generator = Generator(latent_dim, max_len, len(vocab)).to(device)
        generator.load_state_dict(torch.load(path))
        generator.eval()
        logger.info("Generator model loaded!")
        return generator

    def save(self, path: str = "generator.pth") -> None:
        torch.save(self.state_dict(), path)

    def clone(self) -> Generator:
        cloned: Generator = Generator(self.model[0].in_features, self.output_len, self.vocab_size)
        cloned.load_state_dict(self.state_dict())
        return cloned.to(next(self.parameters()).device)

    @staticmethod
    def sample_gumbel_softmax(logits, tau=1.0):
        return F.gumbel_softmax(logits, tau=tau, hard=False, dim=-1)

    def forward(self, z):
        out = self.model(z)
        out = out.view(-1, self.output_len, self.vocab_size)
        return Generator.sample_gumbel_softmax(out, tau=1.0)

class Discriminator(nn.Module):
    def __init__(self, input_len, vocab_size):
        super().__init__()
        self.model = nn.Sequential(
            nn.Flatten(),
            nn.Linear(input_len * vocab_size, 256),
            nn.LeakyReLU(0.2),
            nn.Dropout(0.3),
            nn.Linear(256, 128),
            nn.LeakyReLU(0.2),
            nn.Dropout(0.3),
            nn.Linear(128, 1),
            nn.Sigmoid()
        )

    def forward(self, x):
        return self.model(x)

class WordDataset(Dataset):
    def __init__(self, word_list, vocab, max_len):
        self.vocab: set = vocab
        self.char2idx: dict = { c: i for i, c in enumerate(vocab) }
        self.vocab_size: int = len(vocab)
        self.max_len: int = max_len
        self.encoded: list = [ self.encode(w) for w in word_list ]

    def encode(self, s: str):
        s = s.lower()
        vec = torch.zeros(self.max_len, self.vocab_size)
        for i, c in enumerate(s[:self.max_len]):
            if c in self.char2idx:
                vec[i, self.char2idx[c]] = 1.0

        return vec

    def __len__(self):
        return len(self.encoded)

    def __getitem__(self, idx):
        return self.encoded[idx]

class Trainer:
    def __init__(self, device: str = "cuda") -> None:
        self.device = torch.device(device if torch.cuda.is_available() else "cpu")
        logger.info(f"Trainer device is: {device if torch.cuda.is_available() else 'CPU'}")
        
    def get_device(self):
        return self.device
        
    def get_gd(self, max_len: int, vocab: list, latent_dim: int = 32) -> tuple[Generator, Discriminator]:
        G = Generator(latent_dim, max_len, len(vocab)).to(self.device)
        D = Discriminator(max_len, len(vocab)).to(self.device)
        return G, D

    def train_single_gan(
        self, word_list: list, vocab: set, max_len: int, save_path: str, lr: float, 
        name: str, progress: Progress,
        latent_dim: int = 32, batch_size: int = 32, epochs: int = 1000
    ) -> None:
        logger.info(f"Training starting. Pararms:\nvocab={vocab}, max_len={max_len}, lr={lr}, latent_dim={latent_dim}")
        dataset: WordDataset = WordDataset(word_list, vocab, max_len)
        dataloader: DataLoader = DataLoader(dataset, batch_size=batch_size, shuffle=True)
        total_batches = len(dataloader)
        gen, disc = self.get_gd(max_len=max_len, vocab=vocab, latent_dim=latent_dim)

        bce = nn.BCELoss()
        opt_G = optim.Adam(gen.parameters(), lr=lr)
        opt_D = optim.Adam(disc.parameters(), lr=lr)

        epoch_task = progress.add_task(f"[cyan]Epochs for {name}", total=epochs)
        batch_task = progress.add_task(f"[green]Batches for {name}", total=total_batches)
        
        for epoch in range(epochs):
            progress.reset(batch_task, total=total_batches, description=f"[green]Epoch {epoch + 1}/{epochs}")
            
            d_losses: list = []
            g_losses: list = []
            
            for real in dataloader:
                real = real.to(self.device)
                bs = real.size(0)
                z = torch.randn(bs, latent_dim).to(self.device)
                fake = gen(z)

                real_labels = torch.ones(bs, 1).to(self.device)
                fake_labels = torch.zeros(bs, 1).to(self.device)

                D_real = disc(real)
                D_fake = disc(fake.detach())
                d_loss = bce(D_real, real_labels) + bce(D_fake, fake_labels)

                opt_D.zero_grad()
                d_loss.backward()
                opt_D.step()

                D_fake = disc(fake)
                g_loss = bce(D_fake, real_labels)

                opt_G.zero_grad()
                g_loss.backward()
                opt_G.step()

                d_losses.append(d_loss.item())
                g_losses.append(g_loss.item())                    
                progress.advance(batch_task)
            
            progress.advance(epoch_task)
            
            avg_d_loss = sum(d_losses) / len(d_losses)
            avg_g_loss = sum(g_losses) / len(g_losses)
            
            progress.update(
                epoch_task,
                description=f"[cyan]Epoch {epoch + 1}/{epochs} (d_loss: {avg_d_loss:.4f}, g_loss: {avg_g_loss:.4f})"
            )

        gen.save(path=save_path)

    @staticmethod
    def decode_onehot(onehot_tensor, vocab) -> str:
        idx2char = {i: c for i, c in enumerate(vocab)}
        indices = onehot_tensor.argmax(dim=-1)
        return ''.join([idx2char[i.item()] for i in indices]).strip()

    def generate_word(self, generator: Generator, vocab: set, latent_dim: int) -> str:
        z = torch.randn(1, latent_dim).to(self.device)
        out = generator(z).cpu().detach()[0]
        return Trainer.decode_onehot(out, vocab)


def print_welcome(parser: argparse.ArgumentParser) -> None:
    print("Welcome! Avaliable arguments:\n")
    help_text = parser.format_help()
    options_start = help_text.find("optional arguments:")
    if options_start != -1:
        options_text = help_text[options_start:]
        print(textwrap.indent(options_text, "    "))
    else:
        print(textwrap.indent(help_text, "    "))


if __name__ == "__main__":
    ascii_banner = pyfiglet.figlet_format("GAN Trainer")
    print(ascii_banner)
    
    parser = argparse.ArgumentParser(description="Train 8.3 name GAN")
    
    # === Device setup ===
    parser.add_argument("--device", type=str, required=False, help="Device type for torch (cpu or cuda)")
    parser.add_argument("--name-lr", type=float, required=False, default=0.002, help="Name model learning rate")
    parser.add_argument("--name-epochs", type=int, required=False, default=100, help="Name model learning epochs")
    parser.add_argument("--ext-lr", type=float, required=False, default=0.002, help="Ext model learning rate")
    parser.add_argument("--ext-epochs", type=int, required=False, default=1000, help="Ext model learning epochs")
    
    # === Dataset setup ===
    parser.add_argument("--name-dataset", type=str, required=False, help="Path to the name dataset CSV")
    parser.add_argument("--name-dataset-col", type=str, required=False, default="name", help="Column in name dataset")
    parser.add_argument("--ext-dataset", type=str, required=False, help="Path to the extension dataset CSV")
    parser.add_argument("--ext-dataset-col", type=str, required=False, default="extension", help="Columns in ext dataset")
    
    # === Saving setup ===
    parser.add_argument("--output-name", type=str, required=False, help="Path to output model name file")
    parser.add_argument("--output-ext", type=str, required=False, help="Path to output model ext file")
    args = parser.parse_args()
    if len(sys.argv) == 1:
        print_welcome(parser=parser)
        exit(1)

    logger.info("GAN Trainer setup:")
    logger.info(f"Device={args.device}")
    logger.info(f"Training params: name_lr={args.name_lr}, ext_lr={args.ext_lr}, name_epochs={args.name_epochs}, ext_epochs={args.ext_epochs}")
    logger.info(f"Dataset params: name_dataset={args.name_dataset}[{args.name_dataset_col}], ext_dataset={args.ext_dataset}[{args.ext_dataset_col}]")
    logger.info(f"Output params: name_model={args.output_name}, ext_model={args.output_ext}")

    name_df: pd.DataFrame = pd.read_csv(args.name_dataset)
    ext_df: pd.DataFrame = pd.read_csv(args.ext_dataset)
    
    name_vocab: list = sorted(''.join(set(name_df[args.name_dataset_col].tolist())).lower())
    ext_vocab: list = sorted(''.join(set(ext_df[args.ext_dataset_col].tolist())).lower())

    name_max_len = name_df[args.name_dataset_col].str.len().max()
    ext_max_len = ext_df[args.ext_dataset_col].str.len().max()
    
    name_trainer: Trainer = Trainer(device=args.device)
    ext_trainer: Trainer = Trainer(device=args.device)
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeRemainingColumn()
    ) as progress:
        
        name_thread = threading.Thread(
            target=name_trainer.train_single_gan,
            args=(
                name_df[args.name_dataset_col].tolist(), 
                set(name_vocab), 
                name_max_len, 
                args.output_name, 
                args.name_lr,
                "NAME",
                progress,
                32, 
                32, 
                args.name_epochs
            ),
            daemon=True
        )
        
        ext_thread = threading.Thread(
            target=ext_trainer.train_single_gan,
            args=(
                ext_df[args.ext_dataset_col].tolist(), 
                set(ext_vocab), 
                ext_max_len, 
                args.output_ext, 
                args.ext_lr,
                "EXTENSION",
                progress,
                32, 
                32, 
                args.ext_epochs
            ),
            daemon=True
        )
        
        name_thread.start()
        ext_thread.start()
        
        while name_thread.is_alive() or ext_thread.is_alive():
            name_thread.join(timeout=0.1)
            ext_thread.join(timeout=0.1)
