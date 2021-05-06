package com.dimajix.flowman.studio.model

import com.dimajix.flowman.studio.service


object Converter {
    def of(kernel:service.KernelService) : Kernel = {
        Kernel(kernel.id)
    }

    def of(launcher: service.Launcher) : Launcher = {
        Launcher(launcher.name, launcher.description)
    }
}
