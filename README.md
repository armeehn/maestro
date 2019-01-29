# maestro
A CLI (Command Line Interface) for managing ML experiments for Oberman Lab.

## Overview

The purpose of maestro is to ease the process of running experiments. Whether it be batches or a single script, maestro has an easy-to-use interface for those who have used CLIs before, and even if not, it isn't too hard to navigate.

### Features
1. Easy to use interface
2. Built-in script dispatcher
 1. Ability to block GPUs
 2. Ability to spread processes over GPUs
3. Auto-completion and bash-like file globbing
4. Process monitoring
5. Ability to kill processes

## Dependencies

We make use of [pyfiglet](https://github.com/pwaller/pyfiglet) and ~~[PyInquirer](https://github.com/CITGuru/PyInquirer) (we use the latter heavily for the interface to be able to ask the user questions)~~ I am happy to announce that it now uses [questionary](https://github.com/tmbo/questionary) and the dependency issue is fixed, i.e. we can all use the latest version of iPython.

## ~~Known Issue (nothing serious)~~

~~Now, as it stands, since maestro relies on `PyInquirer` which has an older version `prompt_toolkit` as a dependency, for this to work, we need that version. I am working on a fix. The known problem arises when you need one of the later versions of iPython. However, if you can get away with using version 5.8.0 (I've tested it and it works) then I would suggest doing that until a fix is found.~~ Fixed.

## Features To Come

I plan on still working on maestro to add features like showing the log directory of an ML experiment (hopefully I can get that out soon). If you have any feature or change that you'd like to see implemented or done, please send me an email [here](mailto:alexander.iannantuono@mail.mcgill.ca).
