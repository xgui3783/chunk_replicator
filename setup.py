from setuptools import setup

setup(name='chunk_replicator',
      version='0.1',
      description='Neuroglancer Chunk Replicator',
      author='Xiao Gui',
      author_email='xgui3783@gmail.com',
      packages=['chunk_replicator'],
      install_requires=[
          "neuroglancer_scripts @ git+https://github.com/xgui3783/neuroglancer-scripts.git@3137e94ee27d78378312eb569059965676bd88ef",
          "dataclasses; python_version < '3.7'",
          "tqdm"
      ]
    )