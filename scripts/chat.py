#!/usr/bin/env python

import sys
from src.chatbot.service import ChatbotService


def main():
    bot = ChatbotService()

    print("Mikoshi (type 'exit' to quit)\n")

    while True:
        try:
            user_input = input("You: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nExiting...")
            break

        if user_input.lower() in {"exit", "quit"}:
            print("Goodbye")
            break

        response = bot.chat(user_input)
        print(f"Mikoshi: {response}\n")


if __name__ == "__main__":
    sys.exit(main())