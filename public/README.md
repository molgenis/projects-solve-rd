# Solve-RD Custom UIs

## UIs

| name                | description                                | release                    |
|---------------------|--------------------------------------------|----------------------------|
| `app-getstarted`    | list of all tables by release, patch (tmp) | 2021-04-06                 |
| `app-home-new`      | redesigned home page                       | 2021-04-06                 |
| `app-home-original` | original home page                         | replaced by `app-home-new` |

## Uploading Apps

### `app-home-new`

Zip `config.json` and `index.hml` into a zip file. In Molgenis, navigate to the App Manager plugin and upload the zipped file.

Run the following commands to import the logos.

```shell
mcmd config set host
mcmd add logo -p www/images/solve-rd-logo-icon.png
mcmd add logo -p www/images/Solve-RD.png 
```
