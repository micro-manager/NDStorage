import os
from ndtiff.nd_tiff_current import NDTiffDataset, NDTiffPyramidDataset
from ndtiff.nd_tiff_v2 import NDTiff_v2_0
from ndtiff.ndtiff_v1 import NDTiff_v1


class Dataset:
    """
    Generic class used to figure out which particular opener to hot swap in
    """
    def __new__(cls, dataset_path=None, full_res_only=True, remote_storage_monitor=None):
        ## Datasets currently being collected
        if dataset_path is None:
            # Check if its a multi-res pyramid or regular
            if "GridPixelOverlapX" in remote_storage_monitor.get_summary_metadata():
                obj = NDTiffPyramidDataset.__new__(NDTiffPyramidDataset)
                obj.__init__(full_res_only=full_res_only, remote_storage_monitor=remote_storage_monitor, dataset_path=dataset_path)
            else:
                obj = NDTiffDataset.__new__(NDTiffDataset)
                obj.__init__(remote_storage_monitor=remote_storage_monitor, dataset_path=dataset_path)
            return obj

        # Search for Full resolution dir, check for index
        res_dirs = [
            dI for dI in os.listdir(dataset_path) if os.path.isdir(os.path.join(dataset_path, dI))
        ]
        if "Full resolution" not in res_dirs:
            # Full resolution was removed ND Tiff starting in V2.1 for non stitched
            fullres_path = dataset_path
            # but if it doesn't have an index, than something is wrong
            if "NDTiff.index" not in os.listdir(fullres_path):
                raise Exception('Cannot find NDTiff index')
            # It must be an NDTiff >= 3.0 non-multi-resolution, loaded from disk
            obj = NDTiffDataset.__new__(NDTiffDataset)
            obj.__init__(dataset_path, remote_storage_monitor=remote_storage_monitor)
            return obj
        # So there is a full res folder, now figure out if its v1 or v2
        fullres_path = (
                dataset_path + ("" if dataset_path[-1] == os.sep else os.sep) + "Full resolution")
        if "NDTiff.index" in os.listdir(fullres_path):
            obj = NDTiff_v2_0.__new__(NDTiff_v2_0)
            obj.__init__(dataset_path, full_res_only, remote_storage_monitor=remote_storage_monitor)
            return obj
        else:
            obj = NDTiff_v1.__new__(NDTiff_v1)
            obj.__init__(dataset_path, full_res_only, remote_storage=None)
            return obj

