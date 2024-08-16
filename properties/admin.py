from django.contrib import admin
from django.utils.safestring import mark_safe
from .models import Location, Amenity, Property, Image


# properties starts here 

class ImageInline(admin.TabularInline):
    model = Image
    extra = 1
    readonly_fields = ('image_preview',)

    def image_preview(self, instance):
        if instance.image:
            return mark_safe(f'<img src="{instance.image.url}" width="100" height="100" />')
        return "No image"
    image_preview.short_description = 'Preview'


@admin.register(Property)
class PropertyAdmin(admin.ModelAdmin):
    inlines = [ImageInline]
    list_display = ('title', 'description', 'display_locations', 'display_amenities', 'created_at', 'updated_at', 'image_preview')
    search_fields = ('title', 'description')
    filter_horizontal = ('locations', 'amenities')
    list_filter = ('created_at', 'updated_at', 'locations', 'amenities')
    readonly_fields = ('created_at', 'updated_at', 'image_preview')

    fieldsets = (
        ('Property Details', {
            'fields': ('title', 'description')
        }),
        ('Relationships', {
            'fields': ('locations', 'amenities')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )

    def image_preview(self, obj):
        if obj.images.exists():
            # Generate HTML for each image with flexbox styling
            images_html = ''.join([
                f'<div style="flex: 1 1 auto; margin: 5px;"><img src="{image.image.url}" style="width: auto; height: auto; max-width: 100px; max-height: 100px;" /></div>'
                for image in obj.images.all()
            ])
            return mark_safe(f'<div style="display: flex; flex-wrap: wrap;">{images_html}</div>')
        return 'No image'
    image_preview.short_description = 'Image Preview'

    def display_locations(self, obj):
        """Display locations as a comma-separated list."""
        return ", ".join([location.name for location in obj.locations.all()])
    display_locations.short_description = 'Locations'

    def display_amenities(self, obj):
        """Display amenities as a comma-separated list."""
        return ", ".join([amenity.name for amenity in obj.amenities.all()])
    display_amenities.short_description = 'Amenities'


@admin.register(Location)
class LocationAdmin(admin.ModelAdmin):
    list_display = ('name', 'type', 'latitude', 'longitude','created_at', 'updated_at')
    search_fields = ('name',)
    list_filter = ('type',)
    readonly_fields = ('created_at', 'updated_at')

    fieldsets = (
        ('Location Details', {
            'fields': ('name', 'type')
        }),
        ('Coordinates', {
            'fields': ('latitude', 'longitude'),
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )


@admin.register(Amenity)
class AmenityAdmin(admin.ModelAdmin):
    list_display = ('name','created_at', 'updated_at')
    search_fields = ('name',)


@admin.register(Image)
class ImageAdmin(admin.ModelAdmin):
    list_display = ('property', 'image_preview', 'image_path', 'created_at', 'updated_at')
    search_fields = ('property__title',)
    list_filter = ('property', 'created_at')
    readonly_fields = ('image_preview', 'created_at', 'updated_at')

    fieldsets = (
        ('Image Details', {
            'fields': ('property', 'image', 'image_preview')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )

    def image_preview(self, obj):
        if obj.image:
            return mark_safe(f'<img src="{obj.image.url}" width="100" height="100" />')
        return 'No image'
    image_preview.short_description = 'Image Preview'

    def image_path(self, obj):
        if obj.image:
            return obj.image.url
        return 'No image path'
    image_path.short_description = 'Image Path'