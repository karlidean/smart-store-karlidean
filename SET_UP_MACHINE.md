# 1. Set Up Machine (Once per machine)

> Keep track of how long this takes, and if you encounter any issues and how you resolve them.
> Estimated time: about an hour

## Step 1.1. (10 min) Create a free GitHub account at <https://github.com>.

Student emails get benefits, but you may lose access after graduation.
A permanent email will be yours forever. You can add a second email to your account.

## Step 1.2. (5 min) Set your Machine to Show File Extensions and Show Hidden Files

When working with Python, you need to see file extensions, e.g. fetcher.py and README.md, and you need to see files that your operating system may try to hide, e.g. `.git/`).

On **Windows**:

- Open File Explorer -> View -> Show -> check "File name extensions" and "Hidden items".

## Step 1.3. (30 min) Install Tools

### Install Git

This is the first of a series of tools used by nearly all professional data analysts.
You only have to install them once.

Install **Git** from: <https://git-scm.com/>.

### Install VS Code

Install **VS Code** from <https://code.visualstudio.com/download>.

⚠️ **Windows users:** During installation, check **Add to PATH** (macOS/Linux handle this automatically).
If you forget, **re-install**.

### Install uv

Install `uv` Python virtual environment and dependency manager using the command below:

On **Windows**, open a PowerShell terminal and paste the following command, then hit ENTER:

```powershell
iwr -useb https://astral.sh/uv/install.ps1 | iex
```

### Restart Terminal

After installing all the tools, restart your terminal so `uv` is on the PATH.

## Step 1.4. (5 min) Configure Git identity (use the same email as your GitHub account)

Using your newly-re-opened terminal, edit these commands to use your name and email.
Copy and paste the edited version into your terminal and hit ENTER after each to run them (one at a time) in your terminal:

```bash
git config --global user.name "Your Name"

git config --global user.email "your_email_used_on_github@example.com"

git config --global init.defaultBranch main
```

## Step 1.5. (5 min) Create a Folder to Hold All Your Repositories (One-Time)

**Important:** Do _not_ store code inside **Documents**, **Desktop**, or any folder synced by OneDrive, Dropbox, or iCloud.

In your terminal, type the following commands, one at a time, hitting ENTER after each:

**Windows**

```shell
cd C:\
mkdir Repos
cd Repos
```

This creates a simple, permanent home for all your GitHub projects:

- On Windows, use `C:\Repos`
- On macOS/Linux, use `~/Repos`

## Step 1.6. (5 min) Verify Machine Setup

With your terminal open in your Repos folder, run the following commands, copy and paste them one at a time, hitting ENTER after each:

```bash
git --version
uv --version
code --version
git config --global user.name
git config --global user.email
code .
```

If versions or git config commands do NOT work correctly, re-do the associated installation and configuration steps above.
Work with an AI assistant (e.g. ChatGPT, Claude.ai, or other) to resolve any issues.

If `code .` doesn’t work, re-run the installation and ensure that **Add to PATH** is checked during installation.

When all of these are true, your setup is complete:

- [ ] You can see hidden files and folders.
- [ ] You can see file extensions.
- [ ] All three version commands respond with version numbers.
- [ ] Your Git configuration information is correct.
- [ ] You can open VS Code with `code .`

Congratulations — your professional analytics environment is ready to go!
(Honestly, getting set up correctly IS the hardest part about using Python for analytics - time to take a break and celebrate!)
