# DatabaseFileSizeMaintenance

This project is for development of a stored procedure, which is to be used as an add-on to [Ola Hallengren's maintenance solution](https://ola.hallengren.com), and retains the same standards and conventions in its implementation of parameters.

The `DatabaseFileSizeMaintenance` procedure focuses on managing database file size growth and shrink, based on specified parameters.

## Prerequisites

The procedure requires [Ola Hallengren's CommandExecute procedure](https://ola.hallengren.com/scripts/CommandExecute.sql) in order to run.

If `@LogToTable = 'Y'` is specified, then you must also have the [CommandLog](https://ola.hallengren.com/scripts/CommandLog.sql) table in the same database.

If `@DatabasesInParallel = 'Y'` is specified, then you must also have the [Queue](https://ola.hallengren.com/scripts/Queue.sql) and [QueueDatabase](https://ola.hallengren.com/scripts/QueueDatabase.sql) tables.

## Arguments

TBA

## Remarks

TBA

## Examples

TBA

## License

This project is licensed under the same open-source project as [Ola Hallengren's maintenance solution](https://ola.hallengren.com/license.html).

The DatabaseFileSizeMaintenance procedure is licensed under the MIT license, a popular and widely used open source license.

Copyright (c) 2020 Madeira Data Solutions

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
