# software-mentions-dataset-analysis
Analyses of software mentions and dependencies

## The Protos

To regenerate the protocol buffers, run:

```shell
protoc -I=./pkg --go_out=. $(find . -type f -name "*.proto")
```
