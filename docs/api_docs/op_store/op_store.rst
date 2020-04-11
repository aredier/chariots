chariots Op Store
=================
.. toctree::
   :maxdepth: 2

   savers

.. automodule:: chariots.op_store
   :undoc-members:
   :show-inheritance:
   :members:
   :exclude-members: OpStoreServer, OpStoreClient, BaseOpStoreClient

   .. autoclass::  OpStoreServer

   .. autoclass:: OpStoreClient
      :show-inheritance:
      :members:
           get_all_versions_of_op
           get_validated_links
           get_op_bytes_for_version
           save_op_bytes
           register_valid_link
           pipeline_exists
           register_new_pipeline
