from setuptools import setup

setup(name='chunk_replicator',
      version='0.1',
      description='Neuroglancer Chunk Replicator',
      author='Xiao Gui',
      author_email='xgui3783@gmail.com',
      packages=['chunk_replicator'],
      install_requires=[
          # fix issue with gzip encoding revert to master when https://github.com/HumanBrainProject/neuroglancer-scripts/pull/48 is merged
          "neuroglancer_scripts @ git+https://github.com/xgui3783/neuroglancer-scripts.git@17aeac5cb5e2e71f6679cae7fe84b5a716651aca",
          "dataclasses; python_version < '3.7'",
          "tqdm"
      ]
    )