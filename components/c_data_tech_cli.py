from pyfiglet import Figlet

def data_tech_cli():

    f = Figlet(font="ANSIShadow")
    text = f.renderText(" DAE - CLI")
    text2 = f.renderText("  KAIZEN")

    print("🪼 ", "-"*60, "🪼")
    print()
    for i, line in enumerate(text.splitlines()):
        # Warm gradient: yellow (255, 255, ~0) → orange (255, ~165, 0) → red-ish (255, ~100, 0)
        r = 250
        g = max(100, 165 - i * 10)   # green fades as i increases
        b = 0                        # keep blue at 0 for warm tones
        print(f"\033[38;2;{r};{g};{b}m{line}\033[0m")
    
    for i, line in enumerate(text2.splitlines()):
        # Warm gradient: yellow (255, 255, ~0) → orange (255, ~165, 0) → red-ish (255, ~100, 0)
        r = 250
        g = max(100, 165 - i * 10)   # green fades as i increases
        b = 0                        # keep blue at 0 for warm tones
        print(f"\033[38;2;{r};{g};{b}m{line}\033[0m")

    print("🪼 ", "-"*60, "🪼")
    print()

if __name__ == "__main__":
    data_tech_cli()