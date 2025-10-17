import csv
import sys
import argparse
import pyfiglet
import textwrap

from loguru import logger
from rander import Rander
from gan import Generator, Trainer
from alive_progress import alive_bar
from dataset import DataManager, DatasetInfo
from rich.progress import Progress, TaskID, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn

import threading
from queue import Empty, Queue
from concurrent.futures import ThreadPoolExecutor


def generate_random_dataset(n: int, output_csv: str, seed: int = 1234566789) -> None:
    seen_names = set()
    Rander.set_seed(seed=1234566789)

    with open(output_csv, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["name"])

        with alive_bar(n, title="Generating unique names") as bar:
            for _ in range(n):
                while True:
                    name = Rander.generate_83_name().upper()
                    if name not in seen_names:
                        seen_names.add(name)
                        writer.writerow([name])
                        break
                bar()
                

def _generate_batch(
    trainer: Trainer,
    name_generator: Generator,
    ext_generator: Generator,
    names_vocab: set,
    ext_vocab: set,
    batch_size: int = 1000,
    progress_update_callback=None
) -> list[str]:
    results: list[str] = []
    for _ in range(batch_size):
        name = (
            f"{trainer.generate_word(generator=name_generator, vocab=names_vocab, latent_dim=32)}."
            f"{trainer.generate_word(generator=ext_generator, vocab=ext_vocab, latent_dim=32)}"
        ).upper()
        results.append(name)
        if progress_update_callback:
            progress_update_callback()

    return results


def generate_gan_dataset(
    n: int, trainer: Trainer, name_generator: Generator, ext_generator: Generator,
    names_vocab: set, ext_vocab: set, output_csv: str, num_threads: int = 8, batch_size: int = 1000
) -> None:
    seen_names = set()
    seen_lock = threading.Lock()
    name_queue = Queue()

    with open(output_csv, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["name"])

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeRemainingColumn()
        ) as progress:
            
            total_task: TaskID = progress.add_task("Total Progress", total=n)
            def worker(thread_index: int, progress: Progress) -> None:
                while True:
                    with seen_lock:
                        if len(seen_names) >= n:
                            break
                        
                    task_id = progress.add_task(f"Thread-{thread_index + 1}", total=batch_size)
                    batch = _generate_batch(
                        trainer, name_generator.clone(), ext_generator.clone(), names_vocab, ext_vocab,
                        batch_size=batch_size, progress_update_callback=lambda: progress.advance(task_id, 1)
                    )
                    progress.remove_task(task_id=task_id)
                    name_queue.put((thread_index, batch))
                    
                logger.info(f"Thread-{thread_index + 1} finished")

            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = [ executor.submit(worker, i, progress) for i in range(num_threads) ]
                written = 0
                
                while written < n:
                    try:
                        _, batch = name_queue.get(timeout=15)
                    except Empty:
                        if all(f.done() for f in futures):
                            logger.info("All threads finished")
                            break
                        continue

                    for name in batch:
                        if written >= n:
                            break
                            
                        if name not in seen_names:
                            seen_names.add(name)
                            writer.writerow([name])
                            written += 1
                            progress.advance(total_task, 1)
                
                if written < n:
                    logger.warning(f"Generated only {written} names out of {n}")
                    

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
    ascii_banner = pyfiglet.figlet_format("Dataset Generator")
    print(ascii_banner)
    
    parser = argparse.ArgumentParser(description="Generate unique names using GANs")
    
    # === Main setup ===
    parser.add_argument("--gan", action="store_true", help="Enable GAN-based name generation")
    parser.add_argument("--random", action="store_true", help="Enable random name generation")
    parser.add_argument("--seed", type=int, default=123456789, help="Random seed")
    
    # === Threading setup ===
    parser.add_argument("--threads", type=int, default=8, help="Threads usage")
    parser.add_argument("--batch", type=int, default=1000, help="Generation batch size")
    
    # === GAN setup ===
    parser.add_argument("--device", type=str, required=False, help="Device type for torch (cpu or cuda)")

    parser.add_argument("--name-size", type=int, required=False, default=8, help="Name max len")
    parser.add_argument("--name-model", type=str, required=False, help="Path to the trained name generator model")
    parser.add_argument("--name-vocab", type=str, required=False, help="Path to the name model vocab")
    parser.add_argument("--name-latent-dim", type=int, required=False, default=32, help="Name model latent dimention")

    parser.add_argument("--ext-size", type=int, required=False, default=3, help="Ext max len")
    parser.add_argument("--ext-model", type=str, required=False, help="Path to the trained extension generator model")
    parser.add_argument("--ext-vocab", type=str, required=False, help="Path to the ext model vocab")
    parser.add_argument("--ext-latent-dim", type=int, required=False, default=32, help="Ext model latent dimention")

    # === Saving setup ===
    parser.add_argument("--output", type=str, required=False, help="Path to output CSV file")
    parser.add_argument("--count", type=int, default=1000, help="Number of unique names to generate")
    args = parser.parse_args()
    if len(sys.argv) == 1:
        print_welcome(parser=parser)
        exit(1)
    
    logger.info("Generator setup...")
    logger.info(f"Type: {'GAN' if args.gan else 'RANDOM'}")
    logger.info(f"Threads={args.threads}, batch={args.batch}")
    if args.gan:
        logger.info(f"Vocabs for models: name={args.name_vocab}, ext={args.ext_vocab}")
        logger.info(f"Models setup: name_len={args.name_size}, ext_len={args.ext_size}")
        logger.info(f"Models: name={args.name_model}, ext={args.ext_model}")
        logger.info(f"Device={args.device}")
    else:
        logger.info(f"Random seed for generation: {args.seed}")
    logger.info(f"Saving location: {args.output}")
    logger.info(f"Generation size (lines): {args.count}")
    
    if args.gan:
        trainer: Trainer = Trainer(args.device)

        name_vocab: set = set()
        with open(args.name_vocab, "r") as f:
            data: str = f.read()
            name_vocab = [x for x in data]

        logger.info(f"Name vocab: {name_vocab}")
        name_generator: Generator = Generator.load(
            latent_dim=args.name_latent_dim,
            max_len=args.name_size,
            vocab=name_vocab,
            device=trainer.get_device(),
            path=args.name_model
        )
        
        ext_vocab: set = set()
        with open(args.ext_vocab, "r") as f:
            data: str = f.read()
            ext_vocab = [x for x in data]

        logger.info(f"Ext vocab: {ext_vocab}")
        ext_generator: Generator = Generator.load(
            latent_dim=args.ext_latent_dim,
            max_len=args.ext_size,
            vocab=ext_vocab,
            device=trainer.get_device(),
            path=args.ext_model
        )

        generate_gan_dataset(
            n=args.count,
            trainer=trainer,
            name_generator=name_generator,
            ext_generator=ext_generator,
            names_vocab=name_vocab,
            ext_vocab=ext_vocab,
            output_csv=args.output,
            num_threads=args.threads,
            batch_size=args.batch
        )
    elif args.random:
        generate_random_dataset(n=args.count, output_csv=args.output)
