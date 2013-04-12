
from solarsan import logging
logger = logging.getLogger(__name__)

import urwid
from copy import copy


palette = [
    (None,  'light gray', 'black'),
    ('heading', 'black', 'light gray'),
    ('line', 'black', 'light gray'),
    ('options', 'dark gray', 'black'),
    ('focus heading', 'white', 'dark red'),
    ('focus line', 'black', 'dark red'),
    ('focus options', 'black', 'light gray'),
    ('selected', 'white', 'dark blue')]

focus_map = {
    'heading': 'focus heading',
    'options': 'focus options',
    'line': 'focus line'}


from functools import partial


class MenuButton(urwid.Button):
    def __init__(self, caption, cb):
        super(MenuButton, self).__init__("")

        urwid.connect_signal(self, 'click', cb)

        self._w = urwid.AttrMap(
            urwid.SelectableIcon(
                [u'  \N{BULLET} ', caption], 2,
            ),
            None,
            'selected',
        )


class Choice(urwid.WidgetWrap):
    obj = None
    parent = None
    _on_click = None

    def __init__(self, caption, parent=None,
                 on_click=None):
        if on_click:
            self._on_click = on_click
        if parent:
            self.parent = parent

        super(Choice, self).__init__(MenuButton(caption, self.on_click))
        self.caption = caption

    def on_click(self, button):
        logger.info('on_click: %s', self)

        if self._on_click:
            #self._on_click(button, choice=self)
            self._on_click(button)
        else:
            response = urwid.Text([u'  You chose ', self.caption, u'\n'])
            done = MenuButton(u'Ok', exit_program)
            response_box = urwid.Filler(urwid.Pile([response, done]))
            bottom_boxes.open_box(urwid.AttrMap(response_box, 'options'))


class HorizontalBoxes(urwid.Columns):
    def __init__(self):
        super(HorizontalBoxes, self).__init__([], dividechars=1)

    def open_box(self, box):
        if self.contents:
            del self.contents[self.focus_position + 1:]
        self.contents.append((urwid.AttrMap(box, 'options', focus_map),
                              self.options('given', 30),
                              ))
        self.focus_position = len(self.contents) - 1


def exit_program(key):
    raise urwid.ExitMainLoop()


def main_loop(widget, palette_merge=None):
    global palette
    palette_merged = copy(palette)
    if palette_merge:
        palette_merged.extend(palette_merge)
    return urwid.MainLoop(widget, palette_merged).run()


from solarsan.storage.pool import Pool
from solarsan.storage.device import Devices, Disks, Partitions
from solarsan.storage.device import Disk, Partition, Mirror


class Menu(urwid.WidgetWrap):
    caption = None
    choices = None
    parent = None
    _on_open = None
    _on_click = None

    def get_caption(self):
        #caption = self.caption
        #if not caption:
        #    caption = self.__class__.__name__
        #return caption
        return getattr(self, 'caption', self.__class__.__name__)

    def get_choice(self, caption, on_click=None):
        if not on_click:
            on_click = self.on_click
        return Choice(caption, parent=self, on_click=on_click)

    def get_choices(self):
        return [self.get_choice('Null')]

    def __init__(self, caption=None, choices=None, parent=None,
                 on_open=None, on_click=None):
        if caption is None:
            caption = getattr(self, 'caption', None)
        if caption is None:
            caption = self.get_caption()
        self.caption = caption

        if choices is None:
            choices = getattr(self, 'choices', None)
        if choices is None:
            choices = self.get_choices()
        self.choices = choices

        self.parent = parent
        self._on_open = on_open
        self._on_click = on_click

        super(Menu, self).__init__(
            MenuButton([caption, u"\N{HORIZONTAL ELLIPSIS}"], self.on_open))
        line = urwid.Divider(u'\N{LOWER ONE QUARTER BLOCK}')
        listbox = urwid.ListBox(
            urwid.SimpleFocusListWalker(
                [
                    urwid.AttrMap(urwid.Text([u"\n  ", caption]), 'heading'),
                    urwid.AttrMap(line, 'line'),
                    urwid.Divider(),
                ] + self.choices + [urwid.Divider()]
            )
        )
        self.menu = urwid.AttrMap(listbox, 'options')

    def on_open(self, button):
        bottom_boxes.open_box(self.menu)
        if self._on_open:
            self._on_open(button, menu=self)

    def on_click(self, button, choice=None):
        if self._on_click:
            self._on_click(button, choice=choice, menu=self)


#def get_response_box(text, buttons)
#    response = urwid.Text([u'  You chose ', self.caption, u'\n'])
#    done = MenuButton(u'Ok', exit_program)
#    response_box = urwid.Filler(urwid.Pile([response, done]))
#    return response_box


class DevicesMenu(Menu):
    base_filter = None

    def __init__(self, caption=None, choices=None, parent=None,
                 on_open=None, on_click=None):
        super(DevicesMenu, self).__init__(caption=caption, choices=choices, parent=parent, on_open=on_open, on_click=on_click)

    def get_choices(self):
        base_filter = self.base_filter
        if not base_filter:
            base_filter = {}
        choices = []
        choices.extend([
            Choice(u'%s' % d, on_click=partial(self.on_click, d))
            for d in Devices(base_filter=base_filter)
        ])
        return choices

    #def on_click(self, device, button, choice=None):
    #    #logger.info('Chose item with obj=%s', device)
    #    #response = urwid.Text([u'  You chose ', u'%s' % device, u'\n'])
    #    #done = MenuButton(u'Ok', exit_program)
    #    #response_box = urwid.Filler(urwid.Pile([response, done]))
    #    #bottom_boxes.open_box(urwid.AttrMap(response_box, 'options'))

    #    if self._on_click:
    #        self._on_click(device, button, choice=choice, menu=self)


class RODevicesMenu(DevicesMenu):
    base_filter = dict(is_readonly=True)


class RWDevicesMenu(DevicesMenu):
    base_filter = dict(is_readonly=False)


class PoolMenu(Menu):
    def __init__(self, *args, **kwargs):
        self.obj = kwargs.pop('pool')
        super(PoolMenu, self).__init__(*args, **kwargs)

    #def open(self):
    #    pool_text = [('bold', u'> Selected Pool: '), u'dpool', u'\n']
    #    top_text.set_text(pool_text)

    def get_choices(self):
        choices = []
        choices.extend([Menu('Crap', choices=[])])
        return choices


class AddDeviceMenu(DevicesMenu):
    caption = 'Add Device'
    device = None
    base_filter = None

    def __init__(self, caption=None, choices=None, parent=None,
                 on_open=None, on_click=None):
        super(AddDeviceMenu, self).__init__(caption=caption, choices=choices, parent=parent, on_open=on_open, on_click=on_click)

    #def add_device(self, device, menu=None):
    #    self.device = device
    #    if self.on_click:
    #        self.on_click(device, menu=self)


class AddMirrorVDevMenu(Menu):
    caption = 'Add Mirror'
    devices = None

    def __init__(self, caption=None, choices=None, parent=None,
                 on_open=None, on_click=None):
        self.devices = Mirror()
        super(AddMirrorVDevMenu, self).__init__(caption=caption, choices=choices, parent=parent, on_open=on_open, on_click=on_click)

    def open_menu(self, button):
        super(AddMirrorVDevMenu, self).open_menu(button)
        #pool_text = [('bold', u'> Creating Pool: '), u'omg', u'\n']
        #top_text.set_text(pool_text)

    def get_choices(self):
        choices = []

        for d in self.devices:
            choices.append(Choice(u'%s' % d))
        if self.devices:
            choices.append(urwid.Divider())

        #add_device = AddDeviceMenu(None, on_click=self.on_click)
        add_device = AddDeviceMenu(None, on_click=self.on_click_device)

        done = Choice('Done', on_click=self.on_done)

        choices.extend([add_device, urwid.Divider(), done])
        return choices

    def on_click_device(self, button, choice=None, menu=None):
        #self.devices.append(device)
        bottom_boxes.set_focus_column(1)
        if not menu:
            menu = self
        #super(AddMirrorVDevMenu, self).on_click_device(button, choice=choice, menu=menu)

    def on_done(self, button, choice=None, menu=None):
        #self.devices.append(device)
        bottom_boxes.set_focus_column(1)
        if not menu:
            menu = self
        super(AddMirrorVDevMenu, self).on_done(button, choice=choice, menu=menu)

from storage.device import Cache, Log, Spare


class AddVDevMenu(Menu):
    caption = 'Add Device'

    def __init__(self, caption=None, choices=None, parent=None,
                 on_open=None, on_click=None):
        self.devices = []
        super(AddVDevMenu, self).__init__(caption=caption, choices=choices, parent=parent, on_open=on_open, on_click=on_click)

    def open_menu(self, button):
        super(AddVDevMenu, self).open_menu(button)
        #pool_text = [('bold', u'> Creating Pool: '), u'omg', u'\n']
        #top_text.set_text(pool_text)

    def get_choices(self):
        choices = []

        choices.append(urwid.Divider())

        choices.append(AddMirrorVDevMenu(on_click=self.add_device_cb))

        vdev_types = [Cache, Log, Spare]
        for vt in vdev_types:
            choices.append(Choice(u'Add %s' % vt.__name__))

        return choices

    def add_device_cb(self, device, menu=None):
        self.devices.append(device)


class CreatePoolMenu(Menu):
    caption = 'Create Pool'
    name = 'unnamed'
    pool = None
    devices = None

    def __init__(self, caption=None, choices=None, parent=None,
                 on_open=None, on_click=None):
        self.devices = []
        super(CreatePoolMenu, self).__init__(caption=caption, choices=choices, parent=parent, on_open=on_open, on_click=on_click)
        self.pool = Pool(self.name)
        self.devices = []
        self.set_text()

    def open_menu(self, button):
        super(CreatePoolMenu, self).open_menu(button)
        self.set_text()

    def rename(self, button):
        self.w_name = urwid.Edit(u'Name: ', edit_text=self.name)

        response = self.w_name
        ok = MenuButton(u'Ok', self.rename_cb)

        response_box = urwid.Pile([response, ok])
        response_box = urwid.Filler(response_box)
        #response_box = urwid.Filler(response_box, 'top', top=10)

        bottom_boxes.open_box(urwid.AttrMap(response_box, 'options'))

    def rename_cb(self, button):
        self.name = self.w_name.get_edit_text()
        self.pool.name = self.name
        self.set_text()
        #bottom_boxes.open_box(self.menu)
        bottom_boxes.set_focus_column(0)

    def get_choices(self):
        choices = []

        choices.append(Choice('pdb', on_click=self.pdb))

        rename = MenuButton('Rename', self.rename)
        choices.extend([rename, urwid.Divider()])

        choices.append(AddMirrorVDevMenu(on_click=self.add_device_cb))

        vdev_types = [Cache, Log, Spare]
        for vt in vdev_types:
            choices.append(Choice(u'Add %s' % vt.__name__))

        create = MenuButton('Create', self.create)
        choices.extend([urwid.Divider(), create])

        return choices

    def add_device_cb(self, device, menu=None):
        self.devices.append(device)

    def pdb(self, button):
        from ipdb import set_trace
        set_trace()

    def create(self, button):
        response = urwid.Text(u'Creating pool')

        def null(button):
            pass

        ok = MenuButton(u'Ok', null)
        response_box = urwid.Filler(urwid.Pile([response, ok]))
        bottom_boxes.open_box(urwid.AttrMap(response_box, 'options'))

    def set_text(self):
        """
        Create Pool: {{ name }}

        Devices: [
            {%- for dev in devices %}
            {{ dev }}
            {%- endfor %}
        ]
        """
        context = dict(name=self.name, devices=self.devices)
        tmpl_pre = self.set_text.func_doc
        tmpl = []
        for line in tmpl_pre.splitlines()[1:]:
            tmpl.append(line[8:])
        tmpl = '\n'.join(tmpl)

        text = quick_template(tmpl, context, is_string=True)
        top_text.set_text(text)


class PoolsMenu(Menu):
    caption = 'Pools'

    def get_choices(self):
        choices = []
        choices.extend([CreatePoolMenu()])
        choices.extend([PoolMenu(p.name, pool=p) for p in Pool.list()])
        return choices


from solarsan.template import quick_template


tmpl_context = dict(name='omgpool', devices=['sda'])


def set_text(**context):
    """
    Create Pool: {{ name }}

    Devices: [
        {%- for dev in devices %}
        {{ dev }}
        {%- endfor %}
    ]
    """
    if context:
        tmpl_context.update(context)

    tmpl_pre = set_text.func_doc
    tmpl = []
    for line in tmpl_pre.splitlines()[1:]:
        tmpl.append(line[4:])
    tmpl = '\n'.join(tmpl)

    text = quick_template(tmpl, tmpl_context, is_string=True)
    top_text.set_text(text)


top_text = urwid.Text(u'')
top_text_fill = urwid.Filler(top_text, 'top', min_height=20, top=1, bottom=1)
top_text_fill_pad = urwid.Padding(top_text_fill)

top_cols = (20, urwid.Columns([top_text_fill_pad]))

bottom_boxes = HorizontalBoxes()

#menu_top = PoolsMenu()
menu_top = CreatePoolMenu()

bottom_boxes.open_box(menu_top.menu)

top = urwid.Pile([top_cols, bottom_boxes])
top = urwid.Padding(top, left=2, right=2)

main_loop(top)
