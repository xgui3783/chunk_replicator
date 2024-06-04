from setuptools import setup

setup(name='chunk_replicator',
      version='0.1',
      description='Neuroglancer Chunk Replicator',
      author='Xiao Gui',
      author_email='xgui3783@gmail.com',
      packages=['chunk_replicator'],
      install_requires=[
          "neuroglancer_scripts @ git+https://github.com/HumanBrainProject/neuroglancer-scripts.git@0205370",
          "dataclasses; python_version < '3.7'",
          "tqdm"
      ]
    )