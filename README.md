# cosim 🖧

(C)omputation (O)ffloading (Sim)ulator

---

## Setup

* clone the `cosim` repo and `cd` into it

```shell
git clone https://github.com/NelsonSharma/cosim.git
```

```shell
cd cosim
```

* make a virtual env

```shell
python -m venv .venv
```

* activate venv

```shell
source .venv/bin/activate
```

* install required packages

```shell
python -m pip install numpy Flask waitress matplotlib networkx requests
```

* install `cosim` as package

```shell
python -m pip install -e .
```

* note down the python executable path `.venv/bin/python`

```shell
ls -lash .venv/bin/python
```

---

## Reference

For a complete offloading example with custom workflows and infrastructure, refer the three notebook files in order:

1. Setting up Offloading Infrastructure - [Infra.ipynb](./Infra.ipynb)

2. Setting up workflows with custom tasks -
    * Using `main` calls - [Flow.ipynb](./Flow.ipynb)
    * Using nested calls - [Flows.ipynb](./Flows.ipynb)

3. Setting up Offloading decisions and execution - [Offload.ipynb](./Offload.ipynb)

---
